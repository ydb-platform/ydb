/* syntax version 1 */
/* dq can not */

PRAGMA pq.Consumer="test_client";

SELECT
    value,
    color,
    SystemMetadata("create_time") as create_time,
    SystemMetadata("write_time") as write_time,
    SystemMetadata("partition_id") as partition_id,
    SystemMetadata("offset") as offset,
    SystemMetadata("message_group_id") as message_group_id,
    SystemMetadata("seq_no") as v,
FROM pq.`test_topic_input`
WITH (
    format=json_each_row,
    SCHEMA (
        value String,
        color String
    )
);
