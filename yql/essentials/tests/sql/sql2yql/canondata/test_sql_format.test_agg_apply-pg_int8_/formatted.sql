/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    Pg::count(),
    Pg::count(key),
    Pg::min(key),
    Pg::max(key),
    Pg::sum(key),
    Pg::avg(key)
FROM (
    VALUES
        (1l),
        (2l),
        (3l)
) AS a (
    key
);
