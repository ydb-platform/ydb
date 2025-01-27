/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    count(*)
FROM (values (1),(null),(3)) as a(key)
