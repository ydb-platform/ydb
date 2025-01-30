/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

insert into @tmp
select * from Input order by subkey, key;
commit;

select * from @tmp order by subkey, key;

insert into @tmp with truncate
select key, value, subkey from Input order by subkey, key;

select * from @tmp order by subkey, key;
commit;
select * from @tmp order by subkey, key;

