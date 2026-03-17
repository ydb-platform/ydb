from enum import Enum


class DebugTrail(Enum):
    DISABLE = "DISABLE"
    FIRST = "FIRST"
    ALL = "ALL"


class Direction(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
