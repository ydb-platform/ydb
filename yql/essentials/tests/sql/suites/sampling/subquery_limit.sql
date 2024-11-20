/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 6 */

select * from (select * from plato.Input) tablesample bernoulli(80) limit 5;
