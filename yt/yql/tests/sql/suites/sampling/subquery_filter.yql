/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 8 */

select * from (select key from plato.Input where subkey != "1") tablesample bernoulli(44) where key > "50";
