$p = SELECT
    user,
    HOP_START() as ts,
    SUM(payload) as payload
FROM (select 'foo' as user, cast(1 as Timestamp) as ts, 10 as payload)
GROUP BY HOP(ts, "PT10S", "PT10S", "PT10S"), user;

$p = PROCESS $p;
SELECT FormatType(TypeOf($p)); -- no MultiHoppingCore comp node
