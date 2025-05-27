/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

$src = (
    SELECT
        Date('2022-01-01') AS int32,
        2 AS value
    UNION ALL
    SELECT
        Date('2021-12-31') AS int32,
        1 AS value
);

$with_bytes = (
    SELECT
        t.*,
        ToBytes(int32) AS date_bytes,
        ToBytes(value) AS int_bytes
    FROM
        $src AS t
);

SELECT
    int32,
    value,
    FromBytes(date_bytes, TypeOf(int32)) AS d,
    FromBytes(int_bytes, int32) AS i
FROM
    $with_bytes
ORDER BY
    int32
;
