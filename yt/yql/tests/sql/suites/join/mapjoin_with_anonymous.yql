/* postgres can not */
/* kikimr can not */
use plato;
pragma DisableSimpleColumns;

pragma yt.MapJoinLimit="1m";

insert into @tmp select * from Input where key > "100";
commit;

select * from Input as a
left join @tmp as b on a.key = b.key;
