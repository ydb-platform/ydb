/* postgres can not */
USE plato;

insert into @foo
select 1;

commit;

$name = "fo" || "o";
select * from concat(@foo,@$name);
