/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    count(*),
    count(key)
FROM (
    VALUES
        (1),
        (NULL),
        (3)
) AS a (
    key
);
