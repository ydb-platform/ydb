from enum import Enum, unique


class NodeTag:
    READ = "read"
    WRITE = "write"
    CTE = "cte"
    DROP = "drop"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    SELFLOOP = "selfloop"
    DESTINATION = "destination"


class EdgeTag:
    INDEX = "index"


@unique
class EdgeType(Enum):
    LINEAGE = 1
    RENAME = 2
    HAS_COLUMN = 3
    HAS_ALIAS = 4


class LineageLevel:
    TABLE = "table"
    COLUMN = "column"
