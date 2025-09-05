/* syntax version 1 */
/* postgres can not */
/* hybridfile can not YQL-17764 */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */
use plato;

pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1m";

select * from(
select * from plato.Input as a
inner join plato.Input as b
on a.key = b.key
) tablesample bernoulli(30);
