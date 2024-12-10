/* postgres can not */
USE plato;

$_data = (
    SELECT
        key AS kk,
        subkey AS sk,
        value AS val
    FROM
        plato.Input
    WHERE
        key == '075'
);

$data_one_key = (
    SELECT
        subkey AS sk
    FROM
        plato.Input
    WHERE
        key == '075'
);

SELECT
    *
FROM
    Input
WHERE
    key == $data_one_key
;
