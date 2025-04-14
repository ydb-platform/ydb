CREATE TABLE `/Root/a` (
    -- a Int32 not null,
    kal Int32 not null,
    payload String,
    PRIMARY KEY (kal)
)
WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);

CREATE TABLE `/Root/b` (
    id Int32 not null,
    a Int32 not null,
    aa Int32 not null,
    payload String,
    PRIMARY KEY (id)
) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);