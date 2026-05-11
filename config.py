import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "who_health")
    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "changeme")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class PipelineConfig:
    api_base_url: str = os.getenv(
        "WHO_API_BASE_URL", "https://ghoapi.azureedge.net/api"
    )
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    indicators: tuple[str, ...] = (
        "MDG_0000000003",  # Adolescent birth rate (per 1000 women)
    )
