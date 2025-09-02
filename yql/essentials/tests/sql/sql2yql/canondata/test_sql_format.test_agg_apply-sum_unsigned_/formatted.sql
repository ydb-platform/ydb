/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    sum(key)
FROM (
    VALUES
        (1u),
        (NULL),
        (3u)
) AS a (
    key
);
