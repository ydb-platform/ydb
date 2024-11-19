/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 2 */

$a = select * from plato.Input;

select * from $a tablesample bernoulli(0.1)
