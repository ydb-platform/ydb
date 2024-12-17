/* syntax version 1 */
SELECT
    key,
    Min(subkey) AS subkey,
    Max(value) AS value
FROM
    plato.Input
GROUP BY
    key
ORDER BY
    key
;
