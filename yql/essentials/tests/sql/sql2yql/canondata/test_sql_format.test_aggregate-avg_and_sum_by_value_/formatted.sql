/* syntax version 1 */
SELECT
    avg(CAST(key AS int)) + 0.3 AS key,
    CAST(sum(CAST(subkey AS int)) AS varchar) AS subkey,
    value
FROM plato.Input
GROUP BY
    value
ORDER BY
    value;
