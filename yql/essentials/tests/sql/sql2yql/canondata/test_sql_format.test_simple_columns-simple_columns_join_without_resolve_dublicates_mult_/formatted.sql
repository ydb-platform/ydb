/* postgres can not */
PRAGMA SimpleColumns;
USE plato;

$data = (
    SELECT
        CAST(CAST(key AS uint32) / 100 AS string) AS key,
        key AS kk,
        CAST(subkey AS uint32) * 10 AS subkey,
        "data: " || value AS value
    FROM Input
    WHERE CAST(key AS uint32) / 100 < 5
);

--INSERT INTO Output
SELECT
    Input.*,
    d.*,
    Input.value AS valueFromInput,
    d.subkey AS subkeyFromD
WITHOUT
    Input.value,
    d.subkey,
    d.key
FROM Input
JOIN $data
    AS d
ON Input.subkey == d.key
ORDER BY
    key,
    value;
