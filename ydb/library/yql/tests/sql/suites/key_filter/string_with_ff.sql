/* syntax version 1 */
/* postgres can not */
use plato;

insert into @src
select "\xff\xff" || key as key from Input order by key;

commit;

select count(*) from (
  select * from @src where StartsWith(key, "\xff\xff") and EndsWith(key, "5")
);

