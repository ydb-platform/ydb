/* postgres can not */
/* syntax version 1 */
select * from (select d.*, Just(key) as ok from plato.Input as d) flatten by ok;
