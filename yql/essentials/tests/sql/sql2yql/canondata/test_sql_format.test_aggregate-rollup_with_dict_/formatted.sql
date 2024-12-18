/* syntax version 1 */
/* postgres can not */
USE plato;

--insert into Output
SELECT
    val,
    count(*) AS cnt,
    grouping(val) AS grouping
FROM
    Input AS t
GROUP BY
    ROLLUP (t.`dict`['c'] AS val)
ORDER BY
    val,
    cnt
;
