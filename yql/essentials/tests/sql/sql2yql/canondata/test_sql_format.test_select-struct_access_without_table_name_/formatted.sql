/* postgres can not */
USE plato;

$data = (
    SELECT
        CAST(key AS uint32) % 10 AS mod,
        (key AS kk, subkey AS sk) AS struct_field,
        value
    FROM Input
);

--INSERT INTO Output
SELECT
    mod,
    struct_field.kk AS mod_key,
    struct_field.sk,
    d.value
FROM $data
    AS d
ORDER BY
    mod_key,
    value;
