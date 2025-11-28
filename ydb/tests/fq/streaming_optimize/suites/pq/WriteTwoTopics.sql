/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="10";
PRAGMA pq.Consumer="test_client";

INSERT INTO pq.test_topic_output
    SELECT Data FROM pq.test_topic_input;

COMMIT;

INSERT INTO pq.test_topic_output2
    SELECT Data FROM pq.test_topic_input2;
