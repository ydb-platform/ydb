/* custom error: Expected numeric type, but got String */
$x = (
    SELECT
        just(1)
);

SELECT
    SUM(x)
FROM (
    SELECT
        'foo' AS x,
        1 AS y
)
WHERE
    y IN $x
;
