/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    count(if(key == 1, CAST(key AS string)))
FROM plato.Input;
