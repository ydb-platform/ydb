/* syntax version 1 */
/* dq can not */

PRAGMA dq.MaxTasksPerStage="10";
PRAGMA pq.Consumer="test_client";

$crc = ($s) -> {
    return Unwrap(len($s) % 8);
};

INSERT INTO solomon.`project/cluster/service`
    SELECT STREAM
        Unwrap(HOP_END()) as ts,
        cast(crc as string) as crc8,
        COUNT(*) as count,
        MIN(Len(Data)) as min_length,
        MAX(Len(Data)) as max_length,
        SUM(Len(Data)) as sum
    FROM
        (SELECT
            Data,
            $crc(Data) as crc
        FROM pq.test_topic_input)
    GROUP BY HOP(Just(CurrentUtcTimestamp(TableRow())), "PT5S", "PT5S", "PT5S"), crc;
