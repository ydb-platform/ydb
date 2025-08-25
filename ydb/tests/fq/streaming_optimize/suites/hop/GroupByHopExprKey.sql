/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="2";

PRAGMA pq.Consumer="test_client";

INSERT INTO pq.test_topic_output
SELECT STREAM
    Yson::SerializeText(Yson::From(TableRow()))
FROM (
    SELECT STREAM
        Sum(v) as sum
    FROM (
        SELECT STREAM
            Yson::LookupUint64(ys, "time") as t,
            Yson::LookupInt64(ys, "key") as k,
            Yson::LookupInt64(ys, "val") as v
        FROM (
            SELECT STREAM
                Yson::Parse(Data) AS ys
            FROM pq.test_topic_input))
    GROUP BY
        MIN_OF(k, 2),
        HOP(DateTime::FromMilliseconds(CAST(Unwrap(t) as Uint32)), "PT0.005S", "PT0.01S", "PT0.01S"));
