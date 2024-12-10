/* syntax version 1 */
USE plato;

SELECT DISTINCT
    k || "_" AS k1,
    "_" || v AS v1
FROM
    Input2
GROUP BY
    key AS k,
    value AS v
ORDER BY
    k1,
    v1
;
