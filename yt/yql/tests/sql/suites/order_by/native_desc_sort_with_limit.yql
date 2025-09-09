/* postgres can not */
use plato;
pragma yt.UseNativeDescSort;

insert into Output
select * from Input
order by key, subkey desc limit 3;
