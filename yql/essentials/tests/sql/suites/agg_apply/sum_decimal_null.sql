/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    sum(key)
FROM (values (CAST("1.51" AS Decimal(10, 3))), (null), (CAST("3.49" AS Decimal(10, 3)))) as a(key)