/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="10";
PRAGMA pq.Consumer="test_client";

SELECT * FROM pq.object("test_topic_input", json_each_row) WITH SCHEMA (String as value, String as color);