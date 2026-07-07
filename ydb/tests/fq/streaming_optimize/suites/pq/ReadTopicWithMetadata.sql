/* syntax version 1 */
/* dq can not */

PRAGMA pq.Consumer="test_client";

SELECT
    value,
    color,
    __ydb_create_time as create_time,
    __ydb_write_time as write_time,
    __ydb_partition_id as partition_id,
    __ydb_offset as offset,
    __ydb_message_group_id as message_group_id,
    __ydb_seq_no as v,
FROM pq.`test_topic_input`
WITH (
    format=json_each_row,
    SCHEMA (
        value String,
        color String
    )
);
