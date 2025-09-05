/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */

select key from (select key, value from plato.Input) tablesample bernoulli(33) order by key;

