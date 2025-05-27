/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    sum(key)
FROM (
    VALUES
        (CAST(1 AS Interval)),
        (NULL),
        (CAST(3 AS Interval))
) AS a (
    key
);
