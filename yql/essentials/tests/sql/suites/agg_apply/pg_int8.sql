/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    Pg::count(),Pg::count(key),Pg::min(key),Pg::max(key),Pg::sum(key),Pg::avg(key)
FROM (values (1l),(2l),(3l)) as a(key)
