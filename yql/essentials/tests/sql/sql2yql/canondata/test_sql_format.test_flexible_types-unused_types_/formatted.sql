/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

$format = ($x) -> (FormatType($x));

$src = (
    SELECT
        1 AS integer,
        Float
);

SELECT
    $format(integer) AS formatted,
    integer + 1 AS int_plus_one
FROM
    $src
;
