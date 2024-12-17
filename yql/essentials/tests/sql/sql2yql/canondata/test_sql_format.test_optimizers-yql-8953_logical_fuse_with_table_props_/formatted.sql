USE plato;

SELECT
    key,
    subkey,
    TableName() AS name
FROM
    Input
WHERE
    value == 'q'
;

SELECT
    key,
    count(*) AS subkeys
FROM (
    SELECT DISTINCT
        key,
        subkey
    FROM
        Input
    WHERE
        value == 'q'
)
GROUP BY
    key
;
