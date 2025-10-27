/* syntax version 1 */
/* postgres can not */
use plato;

$src = select null as key, value from Input;
$src_opt = select null as key, Just(value) as value from Input;
$src_null = select null as key, null as value from Input;


select min_by(value, key) from $src;
select max_by(value, key) from $src_opt;
select min_by(value, key) from $src_null;

select max_by(value, key) from (select * from $src limit 0);
select min_by(value, key) from (select * from $src_opt limit 0);
select max_by(value, key) from (select * from $src_null limit 0);


select min_by(value, key) from (select Nothing(String?) as key, value from Input);
