/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

$format = ($x) -> (FormatType($x));

SELECT
    $format(integer) AS formatted,
    $format(inTeger) AS formatted2,
    inTeger AS int,
    inTeger + 1 AS int_plus_one
FROM (
    SELECT
        1 AS inTeger
);
