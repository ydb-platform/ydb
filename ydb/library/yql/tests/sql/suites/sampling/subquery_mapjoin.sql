/* syntax version 1 */
/* postgres can not */
/* hybridfile can not YQL-17764 */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
use plato;

pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1m";

select * from(
select * from plato.Input as a
inner join plato.Input as b
on a.key = b.key
) tablesample bernoulli(30);
