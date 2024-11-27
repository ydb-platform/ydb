/* syntax version 1 */
USE plato;

SELECT
    key
FROM Input3
GROUP BY
    key
HAVING count(DISTINCT subkey || subkey) > 1;
