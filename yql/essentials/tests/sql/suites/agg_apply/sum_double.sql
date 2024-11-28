/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    sum(key)
FROM (values (1.51),(null),(3.49)) as a(key)