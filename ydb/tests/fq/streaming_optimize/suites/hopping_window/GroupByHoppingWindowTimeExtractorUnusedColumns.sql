/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="2";
PRAGMA dq.WatermarksMode="default";
PRAGMA dq.ComputeActorType="async";

PRAGMA pq.Consumer="test_client";

$input =
    SELECT
        *
    FROM pq.test_topic_input
    WITH (
        FORMAT=json_each_row,
        SCHEMA(
            t Uint64,
            k String,
            v Uint64
        )
    );

$output =
    SELECT
        percentile(v, 0.75) AS p75,
        percentile(v, 0.9) AS p90
    FROM $input
    GROUP BY
        HoppingWindow(DateTime::FromMilliseconds(t), "PT0.005S", "PT0.01S");

INSERT INTO pq.test_topic_output
SELECT Yson::SerializeText(Yson::From(TableRow()))
FROM $output;
