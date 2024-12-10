/* postgres can not */
USE plato;

$_data = (
    SELECT
        key AS kk,
        subkey AS sk,
        value AS val
    FROM
        plato.Input1
    WHERE
        key == '075'
);

$data_one_key = (
    SELECT
        subkey
    FROM
        plato.Input1
    WHERE
        key == '075'
);

SELECT
    *
FROM
    Input2
WHERE
    key == $data_one_key
;
