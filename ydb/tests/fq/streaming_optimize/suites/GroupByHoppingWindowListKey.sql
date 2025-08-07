/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="2";
PRAGMA dq.WatermarksMode="default";
PRAGMA dq.ComputeActorType="async";

PRAGMA pq.Consumer="test_client";

INSERT INTO pq.test_topic_output
SELECT
    Yson::SerializeText(Yson::From(TableRow()))
FROM (
    SELECT
        Sum(v) as sum
    FROM pq.test_topic_input
    WITH (
        format=json_each_row,
        schema=(
            t Uint64,
            k Uint64,
            v Uint64
        )
    )
    GROUP BY
        k,
        AsList(1, k),
        HoppingWindow("PT0.005S", "PT0.01S"));
