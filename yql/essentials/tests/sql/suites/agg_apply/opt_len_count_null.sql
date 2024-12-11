/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    count(1/0)
FROM (values (1),(null),(3)) as a(key)
