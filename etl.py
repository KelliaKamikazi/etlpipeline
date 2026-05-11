"""

WHO GHO ETL Pipeline

Extracts adolescent birth rate data from the WHO GHO OData API,
transforms it, and loads it into PostgreSQL.

"""

import argparse
import logging
import sys
import time

import psycopg2
import psycopg2.extras
import requests
from pydantic import BaseModel, Field, field_validator

from config import DatabaseConfig, PipelineConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Pydantic models ──────────────────────────────────────────────────────────


class RawObservation(BaseModel):
    Id: int
    IndicatorCode: str
    SpatialDim: str = ""
    ParentLocationCode: str | None = None
    ParentLocation: str | None = None
    TimeDim: int | None = None
    Dim1Type: str | None = None
    Dim1: str | None = None
    Value: str | None = None
    NumericValue: float | None = None
    Low: float | None = None
    High: float | None = None
    DataSourceDim: str | None = None
    Comments: str | None = None

    @field_validator("TimeDim", mode="before")
    @classmethod
    def coerce_timedim(cls, v):
        if v is None:
            return None
        return int(v)


class TransformedObservation(BaseModel):
    api_id: int
    indicator_code: str = Field(min_length=1)
    country_code: str = Field(min_length=2, max_length=10)
    year: int = Field(ge=1900, le=2100)
    dim1_type: str | None = None
    dim1: str | None = None
    numeric_value: float | None = None
    low: float | None = None
    high: float | None = None
    value_display: str | None = None
    data_source: str | None = None
    comments: str | None = None


# CONSTANTS

TARGET_COUNTRIES = {"RWA", "SGP", "ESP", "USA", "MEX"}

INDICATOR_NAMES = {
    "MDG_0000000003": "Adolescent birth rate (per 1000 women)",
}


# PIPELINE STATE 


def get_pipeline_state(conn, indicator_code):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_offset, status FROM pipeline_state WHERE indicator_code = %s",
            (indicator_code,),
        )
        row = cur.fetchone()
    conn.commit()
    if row:
        return {"last_offset": row[0], "status": row[1]}
    return None


def save_checkpoint(cur, indicator_code, offset, status):
    cur.execute(
        """INSERT INTO pipeline_state
               (indicator_code, last_offset, status, updated_at, completed_at)
           VALUES (%s, %s, %s, NOW(),
                   CASE WHEN %s = 'completed' THEN NOW() ELSE NULL END)
           ON CONFLICT (indicator_code) DO UPDATE SET
               last_offset  = EXCLUDED.last_offset,
               status       = EXCLUDED.status,
               completed_at = CASE WHEN EXCLUDED.status = 'completed' THEN NOW()
                                   ELSE pipeline_state.completed_at END,
               updated_at   = NOW()""",
        (indicator_code, offset, status, status),
    )


def reset_pipeline_state(conn, indicator_code):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline_state WHERE indicator_code = %s",
                (indicator_code,),
            )


# EXTRACT 


def extract_batch(indicator, config, skip):
    url = f"{config.api_base_url}/{indicator}"
    params = {"$top": config.batch_size, "$skip": skip}

    for attempt in range(1, config.max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json().get("value", [])
        except requests.RequestException as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, config.max_retries, exc)
            if attempt == config.max_retries:
                raise
            time.sleep(2**attempt)


# TRANSFORM


def transform(raw_records: list[dict]) -> tuple[dict, dict, list[TransformedObservation]]:
    regions: dict[str, str] = {}
    countries: dict[str, str | None] = {}
    observations: list[TransformedObservation] = []
    skipped = 0

    for raw in raw_records:
        try:
            parsed = RawObservation.model_validate(raw)
        except Exception as exc:
            log.warning("Validation failed, skipping record: %s", exc)
            skipped += 1
            continue

        if not parsed.SpatialDim or parsed.TimeDim is None:
            skipped += 1
            continue

        if parsed.SpatialDim not in TARGET_COUNTRIES:
            skipped += 1
            continue

        if parsed.ParentLocationCode and parsed.ParentLocation:
            regions[parsed.ParentLocationCode] = parsed.ParentLocation

        countries.setdefault(parsed.SpatialDim, parsed.ParentLocationCode)

        try:
            obs = TransformedObservation(
                api_id=parsed.Id,
                indicator_code=parsed.IndicatorCode,
                country_code=parsed.SpatialDim,
                year=parsed.TimeDim,
                dim1_type=parsed.Dim1Type,
                dim1=parsed.Dim1,
                numeric_value=parsed.NumericValue,
                low=parsed.Low,
                high=parsed.High,
                value_display=parsed.Value,
                data_source=parsed.DataSourceDim,
                comments=parsed.Comments,
            )
            observations.append(obs)
        except Exception as exc:
            log.warning("Transform failed, skipping record %d: %s", parsed.Id, exc)
            skipped += 1

    log.info("Batch transform: %d valid, %d skipped", len(observations), skipped)
    return regions, countries, observations


# LOAD


def load_batch(cur, regions, countries, observations):
    for code, name in regions.items():
        cur.execute(
            """INSERT INTO regions (code, name) VALUES (%s, %s)
               ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name""",
            (code, name),
        )

    for code, region_code in countries.items():
        cur.execute(
            """INSERT INTO countries (code, region_code) VALUES (%s, %s)
               ON CONFLICT (code) DO UPDATE SET region_code = EXCLUDED.region_code""",
            (code, region_code),
        )

    rows = [
        (
            o.api_id, o.indicator_code, o.country_code, o.year,
            o.dim1_type, o.dim1, o.numeric_value, o.low, o.high,
            o.value_display, o.data_source, o.comments,
        )
        for o in observations
    ]
    psycopg2.extras.execute_batch(
        cur,
        """INSERT INTO observations
               (api_id, indicator_code, country_code, year,
                dim1_type, dim1, numeric_value, low, high,
                value_display, data_source, comments)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT ON CONSTRAINT uq_observation DO UPDATE SET
               api_id         = EXCLUDED.api_id,
               numeric_value  = EXCLUDED.numeric_value,
               low            = EXCLUDED.low,
               high           = EXCLUDED.high,
               value_display  = EXCLUDED.value_display,
               data_source    = EXCLUDED.data_source,
               comments       = EXCLUDED.comments""",
        rows,
        page_size=500,
    )


# PIPELINE RUNNER


def run_indicator(conn, indicator, pipeline_config, full_refresh):
    log.info("=== Pipeline start: %s ===", indicator)

    state = get_pipeline_state(conn, indicator)

    if full_refresh:
        start_offset = 0
        reset_pipeline_state(conn, indicator)
        log.info("Full refresh mode: extracting from scratch")
    elif state and state["status"] == "completed":
        start_offset = state["last_offset"]
        log.info("Incremental mode: checking for new data from offset %d", start_offset)
    elif state and state["status"] == "in_progress":
        start_offset = state["last_offset"]
        log.info("Resuming interrupted extraction from offset %d", start_offset)
    else:
        start_offset = 0
        log.info("First run: extracting all data")

    indicator_name = INDICATOR_NAMES.get(indicator, indicator)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO indicators (code, name) VALUES (%s, %s)
                   ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name""",
                (indicator, indicator_name),
            )

    offset = start_offset
    total_loaded = 0

    while True:
        batch = extract_batch(indicator, pipeline_config, offset)
        if not batch:
            break

        regions, countries, observations = transform(batch)

        # Data load and checkpoint are in the same transaction, either both
        # commit or neither does, so we never lose track of what was loaded.
        with conn:
            with conn.cursor() as cur:
                if observations:
                    load_batch(cur, regions, countries, observations)
                offset += len(batch)
                save_checkpoint(cur, indicator, offset, "in_progress")

        total_loaded += len(observations)
        log.info("Checkpoint at offset %d (%d records loaded so far)", offset, total_loaded)

    with conn:
        with conn.cursor() as cur:
            save_checkpoint(cur, indicator, offset, "completed")

    if total_loaded > 0:
        log.info("=== Pipeline complete: %d new records loaded ===", total_loaded)
    else:
        log.info("=== No new data for %s ===", indicator)


# MAIN 


def main():
    parser = argparse.ArgumentParser(description="WHO GHO ETL Pipeline")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore checkpoints and re-extract all data from scratch",
    )
    args = parser.parse_args()

    db_config = DatabaseConfig()
    pipeline_config = PipelineConfig()
    log.setLevel(pipeline_config.log_level)

    conn = psycopg2.connect(db_config.dsn)
    try:
        for indicator in pipeline_config.indicators:
            try:
                run_indicator(conn, indicator, pipeline_config, args.full_refresh)
            except Exception as exc:
                log.error("Pipeline failed for %s: %s", indicator, exc)
                sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
