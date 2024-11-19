/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
use plato;
pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1m";

select * from plato.Input as a SAMPLE 0.3
inner join plato.Input as b
on a.key = b.key;
