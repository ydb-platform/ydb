/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    some(key)
FROM (
    VALUES
        (1)
) AS a (
    key
);
