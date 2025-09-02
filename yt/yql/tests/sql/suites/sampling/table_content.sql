/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) == 1 */
use plato;

$key = (select key from plato.Input SAMPLE(0.5));

select * from Input where key = $key;
