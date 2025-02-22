/* postgres can not */
select Math::Round(median(val), -3) as med, Math::Round(stddev(val), -3) as dev from (select cast(value as int) as val from plato.Input);