/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;

$foo =
    SELECT
        1 AS sk,
        "150" AS key,
        2 AS v
;

SELECT
    *
FROM
    $foo AS b
JOIN
    Input AS a
USING (key);

SELECT
    a.*
FROM
    $foo AS b
JOIN
    Input AS a
USING (key);

SELECT
    b.*
FROM
    $foo AS b
JOIN
    Input AS a
USING (key);

SELECT
    a.*,
    b.*
FROM
    $foo AS b
JOIN
    Input AS a
USING (key);
