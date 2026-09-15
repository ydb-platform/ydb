/* custom error: Expected numeric type, but got String */
$x = select just(1);

SELECT
    SUM(x)
FROM
    (select 'foo' as x, 1 as y)
WHERE
    y IN $x
;
