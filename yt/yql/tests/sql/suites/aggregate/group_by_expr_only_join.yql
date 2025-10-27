/* syntax version 1 */
/* postgres can not */
select aggregate_list(a.k), aval from (select cast(subkey as uint32) as k, value as val from plato.Input) as a left only join (select cast(key as uint32) as k, cast(subkey as uint32) as s from plato.Input) as b using(k) group by a.val as aval;
