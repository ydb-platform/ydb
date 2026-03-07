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


ALTER OBJECT `/Root/testdb/kv` (TYPE TABLE) SET (
  ACTION = UPSERT_OPTIONS,
ALTER OBJECT `path/to/table` (TYPE TABLE) SET (
  ACTION = UPSERT_OPTIONS,
  `COMPACTION_PLANNER.CLASS_NAME` = 'lc-buckets',
  `COMPACTION_PLANNER.FEATURES` = '{"levels":[
    {"class_name":"Zero","portions_live_duration":"60s","expected_blobs_size":1048576},
    {"class_name":"Zero","expected_blobs_size":2097152},
    {"class_name":"OneLayer","expected_portion_size":2097152},
    {"class_name":"OneLayer","expected_portion_size":4194304},
    {"class_name":"OneLayer","expected_portion_size":6291456},
    {"class_name":"OneLayer","expected_portion_size":8388608}
  ]}'
););