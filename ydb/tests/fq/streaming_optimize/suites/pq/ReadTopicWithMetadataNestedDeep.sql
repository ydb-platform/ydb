SELECT
    value,
    color,
    CAST(__ydb_offset AS String) AS offset
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE',
        format = json_each_row,
        SCHEMA (value String, color String)
    )
;
