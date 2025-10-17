--!syntax_pg
select min(aggfnoid) as minaggfnoid, max(aggfnoid) as maxaggfnoid,
       min(aggkind) as minaggkind, max(aggkind) as maxaggkind,
       min(aggtranstype) as minaggtranstype, max(aggtranstype) as maxaggtranstype from pg_aggregate;

