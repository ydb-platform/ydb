PRAGMA DisableSimpleColumns;

/* postgres can not */
USE plato;

$data = (
    SELECT
        CAST(key AS uint32) % 10 AS mod,
        (key AS kk, subkey AS sk) AS struct_field
    FROM
        Input
);

--INSERT INTO Output
SELECT
    mod,
    struct_field.kk AS mod_key,
    key,
    value
FROM
    Input
JOIN
    $data AS d
ON
    CAST(Input.key AS uint32) / 100 == d.mod
ORDER BY
    key,
    mod_key,
    value
;
