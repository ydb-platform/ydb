/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

$groupsrc = (
    SELECT
        1 AS int32,
        2 AS value
    UNION ALL
    SELECT
        1 AS int32,
        1 AS value
);

SELECT
    int32,
    max(value) AS maxval,
    min(value) AS minval,
FROM
    $groupsrc
GROUP BY
    int32
ASSUME ORDER BY
    int32
;
