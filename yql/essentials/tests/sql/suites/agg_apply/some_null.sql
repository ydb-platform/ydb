/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    some(key)
FROM (values (null)) as a(key)