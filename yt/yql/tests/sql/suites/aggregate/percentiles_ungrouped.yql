/* postgres can not */
select median(val) as med, percentile(val, 0.8) as p80 from (select cast(value as int) as val from plato.Input);