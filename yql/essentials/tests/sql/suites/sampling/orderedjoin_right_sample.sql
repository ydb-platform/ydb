/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
/* ignore plan diff */
/* syntax version 1 */
use plato;
pragma yt.JoinMergeTablesLimit="2";
pragma DisableSimpleColumns;

select * from plato.Input as a
inner join plato.Input as b SAMPLE 0.3
on a.key = b.key;
