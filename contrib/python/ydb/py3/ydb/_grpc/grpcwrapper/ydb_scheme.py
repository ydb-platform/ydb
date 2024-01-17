import datetime
import enum
from dataclasses import dataclass
from typing import List


@dataclass
class Entry:
    name: str
    owner: str
    type: "Entry.Type"
    effective_permissions: "Permissions"
    permissions: "Permissions"
    size_bytes: int
    created_at: datetime.datetime

    class Type(enum.IntEnum):
        UNSPECIFIED = 0
        DIRECTORY = 1
        TABLE = 2
        PERS_QUEUE_GROUP = 3
        DATABASE = 4
        RTMR_VOLUME = 5
        BLOCK_STORE_VOLUME = 6
        COORDINATION_NODE = 7
        COLUMN_STORE = 12
        COLUMN_TABLE = 13
        SEQUENCE = 15
        REPLICATION = 16
        TOPIC = 17


@dataclass
class Permissions:
    subject: str
    permission_names: List[str]
