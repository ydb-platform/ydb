/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 2 */

$a = select * from plato.Input;

select * from $a tablesample bernoulli(0.1)
