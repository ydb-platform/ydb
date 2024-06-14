/* syntax version 1 */
/* postgres can not */
select sum(length(value)), vf, kf, kl, grouping(vf, kf, kl) as ggg3 from plato.Input group by Substring(value, 0, 1) as vf, cube(cast(key as uint32) % 10u as kl, cast(key as uint32) / 100u as kf)
order by vf, kf, kl;
