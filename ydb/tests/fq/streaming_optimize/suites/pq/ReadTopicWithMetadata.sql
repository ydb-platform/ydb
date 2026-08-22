SELECT
    value,
    color,
    __ydb_create_time AS create_time,
    __ydb_write_time AS write_time,
    __ydb_partition_id AS partition_id,
    __ydb_offset AS offset,
    __ydb_message_group_id AS message_group_id,
    __ydb_seq_no AS v,
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE',
        format = json_each_row,
        SCHEMA (value String, color String)
    )
;
