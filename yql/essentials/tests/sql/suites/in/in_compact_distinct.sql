/* postgres can not */
use plato;

select * from Input
where key in compact (select distinct key from Input1)
order by key;
