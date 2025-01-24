/* kikimr can not */
use plato;

PRAGMA DisableSimpleColumns;
PRAGMA yt.JoinCollectColumnarStatistics="async";
PRAGMA yt.MinTempAvgChunkSize="0";
PRAGMA yt.MapJoinLimit="1";

SELECT *
FROM (select distinct key,subkey from Input where cast(key as Int32) > 100 order by key limit 100) as a
RIGHT JOIN (select key,value  from Input where cast(key as Int32) < 500) as b
USING(key)
ORDER BY a.key,a.subkey,b.value;

