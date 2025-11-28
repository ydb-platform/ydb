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
            k Uint64,
            v Uint64
        )
    );

$output =
    SELECT
        Sum(v) AS sum
    FROM $input
    GROUP BY
        MIN_OF(k, 2),
        HoppingWindow(DateTime::FromMilliseconds(t), "PT0.005S", "PT0.01S");

INSERT INTO pq.test_topic_output
SELECT Yson::SerializeText(Yson::From(TableRow()))
FROM $output;
