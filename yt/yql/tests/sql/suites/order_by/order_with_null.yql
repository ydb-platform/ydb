/* postgres can not */
use plato;

$input = (
select null as key, "0" as subkey, "kkk" as value
union all
select * from Input
);

select * from $input order by key asc;
select * from $input order by key desc;
