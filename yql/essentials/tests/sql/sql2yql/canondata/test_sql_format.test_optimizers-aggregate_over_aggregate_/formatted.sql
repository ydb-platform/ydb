/* syntax version 1 */
USE plato;

SELECT DISTINCT
    *
FROM
    Input
GROUP BY
    value,
    subkey,
    key
ORDER BY
    subkey
;
