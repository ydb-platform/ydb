USE plato;

INSERT INTO Output
SELECT
    key,
    '1' AS subkey,
    value || "a" AS value
FROM
    Input
WHERE
    key < "100"
ORDER BY
    value
LIMIT 3;
