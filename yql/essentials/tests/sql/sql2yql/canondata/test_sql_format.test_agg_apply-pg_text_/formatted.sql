/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    Pg::count(),
    Pg::count(key),
    Pg::min(key),
    Pg::max(key),
    Pg::string_agg(key, "|"u)
FROM (
    VALUES
        ("a"u),
        ("b"u),
        ("c"u)
) AS a (
    key
);
