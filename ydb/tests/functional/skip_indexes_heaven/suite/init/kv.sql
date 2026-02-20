CREATE TABLE `/Root/testdb/kv` (
    k           Int64 NOT NULL,
    v_no_index  Int64 NOT NULL,
    v_minmax    Int64 NOT NULL,
    v_bloom     Int64 NOT NULL,
    string_data String NOT NULL,
    PRIMARY KEY (k)
)
PARTITION BY HASH (k)
WITH (STORE=COLUMN);

ALTER OBJECT `/Root/testdb/kv` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_v_minmax, TYPE=MINMAX,
                    FEATURES=`{"column_name" : "v_minmax"}`);

ALTER OBJECT `/Root/testdb/kv` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_v_bloom_filter, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "v_bloom", "false_positive_probability" : 0.01}`);
