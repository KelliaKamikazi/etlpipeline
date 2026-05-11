CREATE TABLE IF NOT EXISTS regions (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS countries (
    code        VARCHAR(10) PRIMARY KEY,
    region_code VARCHAR(10) REFERENCES regions(code)
);

CREATE TABLE IF NOT EXISTS indicators (
    code VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    id              BIGSERIAL PRIMARY KEY,
    api_id          BIGINT       NOT NULL,
    indicator_code  VARCHAR(50)  NOT NULL REFERENCES indicators(code),
    country_code    VARCHAR(10)  NOT NULL REFERENCES countries(code),
    year            SMALLINT     NOT NULL,
    dim1_type       VARCHAR(50),
    dim1            VARCHAR(50),
    numeric_value   NUMERIC(15,6),
    low             NUMERIC(15,6),
    high            NUMERIC(15,6),
    value_display   VARCHAR(100),
    data_source     VARCHAR(255),
    comments        TEXT,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_observation
        UNIQUE NULLS NOT DISTINCT (indicator_code, country_code, year, dim1)
);

CREATE INDEX IF NOT EXISTS ix_obs_indicator ON observations(indicator_code);
CREATE INDEX IF NOT EXISTS ix_obs_country   ON observations(country_code);
CREATE INDEX IF NOT EXISTS ix_obs_year      ON observations(year);

CREATE TABLE IF NOT EXISTS pipeline_state (
    indicator_code VARCHAR(50) PRIMARY KEY,
    last_offset    INT          NOT NULL DEFAULT 0,
    status         VARCHAR(20)  NOT NULL DEFAULT 'in_progress',
    started_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
