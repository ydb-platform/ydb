/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

insert into Input
select key, subkey, value from Input;

commit;

select * from Input order by subkey, key;

insert into Output
select * from Input order by subkey, key;

commit;

select * from Output order by subkey, key;

insert into Output with truncate
select key,value,subkey from Input order by subkey, key;

select * from Output order by subkey, key;
commit;
select * from Output order by subkey, key;

