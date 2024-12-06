/* syntax version 1 */
SELECT
    value,
    avg(CAST(key AS int)) + 0.3 AS key
FROM plato.Input
GROUP BY
    value
HAVING value > "foo"
ORDER BY
    key;
