/* postgres can not */
/* kikimr can not */
use plato;
pragma yt.MapJoinLimit="1M";
pragma yt.TmpFolder="//custom_tmp";

-- MapJoin with table content
$input = (select cast(a.key as Uint64) as key from Input as a cross join Input as b);

-- ResFill with table content
select sum(key) from $input;
