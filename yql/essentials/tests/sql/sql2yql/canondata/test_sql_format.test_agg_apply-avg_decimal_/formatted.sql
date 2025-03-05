/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    avg(key)
FROM (
    VALUES
        (Decimal('0.1', 10, 1)),
        (Decimal('0.2', 10, 1)),
        (Decimal('0.3', 10, 1))
) AS a (
    key
);
