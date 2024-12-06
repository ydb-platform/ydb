/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    *
FROM Input
GROUP BY
    TableRow().key AS k
ORDER BY
    k;
