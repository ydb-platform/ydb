/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */

select key from (select key, value from plato.Input) tablesample bernoulli(33) order by key;

