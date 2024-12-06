/* postgres can not */
PRAGMA SimpleColumns;
USE plato;

$data = (
    SELECT
        key,
        subkey AS sk,
        value
    FROM
        Input
    WHERE
        CAST(key AS uint32) / 100 < 5
);

--INSERT INTO Output
SELECT
    d.*,
    subkey
FROM
    Input
JOIN
    $data AS d
ON
    Input.key == d.key AND Input.value == d.value
ORDER BY
    key,
    value
;
