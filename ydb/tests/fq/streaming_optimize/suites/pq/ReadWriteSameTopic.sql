/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="2";

PRAGMA pq.Consumer="test_client";

INSERT INTO pq.test_topic_input
SELECT random_column_name
FROM pq.test_topic_input
WITH (
    format=raw,
    schema = (
        random_column_name STRING not null
    )
);