CREATE TABLE `/Root/testdb/kv` (
    k           Int64 NOT NULL,
    ints_no_index  Int64 NOT NULL,
    ints_minmax    Int64 NOT NULL,
    strings_no_index String NOT NULL,
    strings_minmax String NOT NULL,
    needle_in_a_haystack_minmax Int64 NOT NULL,
    PRIMARY KEY (k)
)
PARTITION BY HASH (k)
WITH (STORE=COLUMN);

ALTER OBJECT `/Root/testdb/kv` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ints_minmax, TYPE=MINMAX,
                    FEATURES=`{"column_name" : "ints_minmax"}`);


ALTER OBJECT `/Root/testdb/kv` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_strings_minmax, TYPE=MINMAX,
                    FEATURES=`{"column_name" : "strings_minmax"}`);


ALTER OBJECT `/Root/testdb/kv` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_needle_in_a_haystack_minmax, TYPE=MINMAX,
                    FEATURES=`{"column_name" : "needle_in_a_haystack_minmax"}`);
