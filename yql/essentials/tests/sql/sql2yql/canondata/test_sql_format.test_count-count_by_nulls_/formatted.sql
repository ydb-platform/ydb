SELECT
    count(CAST(subkey AS int)) AS not_null_subkeys
FROM
    plato.Input
;
