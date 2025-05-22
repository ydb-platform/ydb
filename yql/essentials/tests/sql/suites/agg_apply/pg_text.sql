/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    Pg::count(),Pg::count(key),Pg::min(key),Pg::max(key),Pg::string_agg(key,"|"u)
FROM (values ("a"u),("b"u),("c"u)) as a(key)
