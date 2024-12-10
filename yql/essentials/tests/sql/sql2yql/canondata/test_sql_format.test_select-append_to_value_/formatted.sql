/* postgres can not */
SELECT
    key,
    subkey,
    value || "foo" AS new_value
FROM
    plato.Input
;
