/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="10";
PRAGMA pq.Consumer="test_client";

INSERT INTO pq.test_topic_output
    SELECT STREAM Data FROM pq.test_topic_input;
