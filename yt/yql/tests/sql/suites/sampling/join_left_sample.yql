/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */
/* ignore plan diff */
use plato;
pragma DisableSimpleColumns;

select * from plato.Input as a SAMPLE 0.3
inner join plato.Input as b
on a.key = b.key;
