/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="10";
PRAGMA pq.Consumer="test_client";

SELECT STREAM Data FROM pq.test_topic_input;
