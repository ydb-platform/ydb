/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    sum(key)
FROM (
    VALUES
        (1.51),
        (NULL),
        (3.49)
) AS a (
    key
);
