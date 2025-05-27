/* syntax version 1 */
/* postgres can not */
$s = (
    SELECT
        1 AS x,
        2 AS y
);

SELECT
    x AS x2,
    y
FROM
    $s
GROUP BY
    ROLLUP (x, y)
ORDER BY
    x2,
    y
;
