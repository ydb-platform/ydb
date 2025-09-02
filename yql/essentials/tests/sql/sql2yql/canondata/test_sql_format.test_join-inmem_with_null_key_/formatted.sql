/* postgres can not */
$a = (
    SELECT
        NULL AS a,
        1 AS b
);

$b = (
    SELECT
        NULL AS a,
        1 AS b
);

SELECT
    a.*
FROM
    $a AS a
LEFT ONLY JOIN
    $b AS b
USING (a);
