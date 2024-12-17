/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    some(key)
FROM (
    VALUES
        (NULL)
) AS a (
    key
);
