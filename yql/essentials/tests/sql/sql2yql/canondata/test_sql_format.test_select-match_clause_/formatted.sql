/* postgres can not */
/* syntax version 1 */
SELECT
    key,
    subkey,
    value
FROM
    plato.Input
WHERE
    value MATCH "q"
;
