CREATE TABLE `test_table_1` (
    key_1   Utf8 NOT NULL,
    key_2   Int64 NOT NULL,
    data_1  Utf8,
    data_2  Uint32,
    PRIMARY KEY (key_1, key_2)
)
PARTITION BY HASH (key_1, key_2)
WITH (STORE=COLUMN);

CREATE TABLE `test_table_2` (
    key_3   Uint32 NOT NULL,
    key_4   Utf8 NOT NULL,
    data_3  Int32,
    data_4  Uint64,
    PRIMARY KEY (key_3, key_4)
)
PARTITION BY HASH (key_3, key_4)
WITH (STORE=COLUMN);

CREATE TABLE `test_table_3` (
    a Uint64,
    b Uint64,
    c String,
    d Date,
    PRIMARY KEY (a)
)
WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 512);
