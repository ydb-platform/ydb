/* postgres can not */
PRAGMA sampleselect;

SELECT
    key,
    subkey,
    value || "foo" AS new_value
FROM
    plato.Input
;
