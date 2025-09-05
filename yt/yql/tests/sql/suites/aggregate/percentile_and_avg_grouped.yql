/* postgres can not */
select key, median(val) as med, avg(val) as avg from (select key, cast(value as int) as val from plato.Input) group by key order by key;