/* syntax version 1 */
USE plato;

SELECT DISTINCT
    *
FROM (
    SELECT
        Unwrap(CAST(key AS Int32)) AS key,
        value
    FROM
        Input2
) AS a
JOIN (
    SELECT
        Just(1ul) AS key,
        123 AS subkey
) AS b
USING (key)
ORDER BY
    value
;
