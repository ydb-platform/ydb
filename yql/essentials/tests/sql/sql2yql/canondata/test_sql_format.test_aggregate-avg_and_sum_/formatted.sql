/* syntax version 1 */
SELECT
    avg(CAST(key AS int)) AS key,
    CAST(sum(CAST(subkey AS int)) AS varchar) AS subkey,
    min(value) AS value
FROM
    plato.Input
;
