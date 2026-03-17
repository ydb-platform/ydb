class NodeTag:
    READ = "read"
    WRITE = "write"
    CTE = "cte"
    DROP = "drop"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    SELFLOOP = "selfloop"


class EdgeTag:
    INDEX = "index"


class EdgeType:
    LINEAGE = "lineage"
    RENAME = "rename"
    HAS_COLUMN = "has_column"
    HAS_ALIAS = "has_alias"


class EdgeDirection:
    IN = "in"
    OUT = "out"


class LineageLevel:
    TABLE = "table"
    COLUMN = "column"
