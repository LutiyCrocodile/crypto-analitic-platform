from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()

@dataclass
class Settings:
    DB_HOST: str = os.environ.get("DB_HOST")
    DB_PORT: str = os.environ.get("DB_PORT")
    DB_USER: str = os.environ.get("DB_USER")
    DB_PASSWORD: str = os.environ.get("DB_PASSWORD")
    DB_NAME: str = os.environ.get("DB_NAME")
    DB_REAL_ESTATE_SCHEMA_NAME: str = os.environ.get("DB_REAL_ESTATE_SCHEMA_NAME")
    DB_CRYPTO_SCHEMA_NAME : str = os.environ.get('DB_CRYPTO_SCHEMA_NAME')

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

settings = Settings()