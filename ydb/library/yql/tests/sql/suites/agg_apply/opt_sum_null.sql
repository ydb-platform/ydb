/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    sum(null)
FROM (values (1),(null),(3)) as a(key)
