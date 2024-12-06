PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data = (
    SELECT
        key AS kk,
        subkey AS sk,
        value
    FROM
        Input
    WHERE
        CAST(key AS uint32) / 100 < 5
);

--INSERT INTO Output
SELECT
    *
WITHOUT
    d.value
FROM
    Input
JOIN
    $data AS d
ON
    Input.subkey == CAST(CAST(d.kk AS uint32) / 100 AS string)
ORDER BY
    key
;
