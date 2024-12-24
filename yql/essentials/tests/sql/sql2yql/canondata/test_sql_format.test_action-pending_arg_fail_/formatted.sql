/* syntax version 1 */
/* postgres can not */
/* custom error:Failed to evaluate unresolved argument: row. Did you use a column?*/
USE plato;

SELECT
    ListExtract(value, key)
FROM
    Input
;
