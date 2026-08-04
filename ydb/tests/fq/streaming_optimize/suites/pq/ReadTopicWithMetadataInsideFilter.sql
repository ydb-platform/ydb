SELECT
    offset
FROM (
    SELECT
        __ydb_offset AS offset
    FROM
        pq.test_topic_input WITH (
            STREAMING = 'TRUE',
            format = json_each_row,
            SCHEMA (value String, color String)
        )
);
