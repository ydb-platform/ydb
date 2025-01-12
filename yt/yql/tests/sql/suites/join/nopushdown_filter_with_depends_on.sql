/* postgres can not */
/* hybridfile can not  */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 4 */
use plato;

-- should not pushdown
select * from Input1 as a left semi join Input2 as b using(key) where Random(TableRow()) < 0.1 order by key;

