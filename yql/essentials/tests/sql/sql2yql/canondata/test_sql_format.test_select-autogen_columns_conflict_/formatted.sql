/* dq can not */
/* dqfile can not */
/* yt can not */
/* postgres can not */
$src = (
    SELECT
        <|column0: 1|> AS x
);

$src = (
    SELECT
        *
    FROM
        $src
        FLATTEN COLUMNS
);

SELECT
    2,
    a.*
FROM
    $src AS a
;

SELECT
    2,
    1 AS column0
;

SELECT
    2,
    1 AS column0
UNION ALL
SELECT
    4,
    3 AS column0
;

SELECT
    *
FROM (
    SELECT
        1
) AS a
JOIN (
    SELECT
        1
) AS b
USING (column0);

SELECT
    1 AS a,
    2 AS b,
    3, -- should be column2
    4 AS column1
;
