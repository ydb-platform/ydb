/* postgres can not */
PRAGMA SimpleColumns;
USE plato;

$data = (
    SELECT
        key AS kk,
        subkey AS sk,
        "data: " || value AS value
    FROM Input
    WHERE CAST(key AS uint32) / 100 < 5
);

--INSERT INTO Output
SELECT
    Input.*,
    d.*,
    Input.value AS valueFromInput
WITHOUT
    Input.value
FROM Input
JOIN $data
    AS d
ON Input.subkey == CAST(CAST(d.kk AS uint32) / 100 AS string)
ORDER BY
    key,
    value;
