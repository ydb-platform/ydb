/* postgres can not */
/* kikimr can not */
use plato;
PRAGMA DisableSimpleColumns;
pragma yt.LookupJoinLimit="64k";
pragma yt.LookupJoinMaxRows="100";
pragma yt.QueryCacheMode="normal";
pragma yt.QueryCacheUseForCalc="true";

insert into @tmp with truncate
select * from Input
where subkey == "bbb"
order by key;

commit;

select * from Input as a
inner join @tmp as b on a.key = b.key
order by a.key, a.subkey;
