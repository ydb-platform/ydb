from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import Field
from pydantic.main import BaseModel

from beanie.odm.documents import Document


class MigrationLog(Document):
    ts: datetime = Field(default_factory=datetime.now)
    name: str
    is_current: bool

    class Settings:
        name = "migrations_log"


class RunningDirections(str, Enum):
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


class RunningMode(BaseModel):
    direction: RunningDirections
    distance: int = 0


class ParsedMigrations(BaseModel):
    path: str
    names: List[str]
    current: Optional[MigrationLog] = None
