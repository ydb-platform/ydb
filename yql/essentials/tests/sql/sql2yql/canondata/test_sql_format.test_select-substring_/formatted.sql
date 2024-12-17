/* postgres can not */
/* syntax version 1 */
SELECT
    substring(key, 1, 1) AS char,
    substring(value, 1) AS tail
FROM
    plato.Input
;
