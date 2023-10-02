--!syntax_pg
select 
    count(*),count(0),count(null),count(null::text),
    min(5),min('a'),min(1.0),min(null),min(null::text),
    max(6),max('a'),max(1.0),max(null),max(null::text),
    sum(7),sum(1.0),sum(null::int)