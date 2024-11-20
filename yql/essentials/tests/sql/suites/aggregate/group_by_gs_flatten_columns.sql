/* syntax version 1 */
/* postgres can not */
use plato;

$input = select a.*, <|k1:1, k2:2|> as s from Input as a;

select key, subkey, some(k1) as k1, some(k2) as k2
from $input flatten columns
group by grouping sets ((key), (key, subkey))
order by key, subkey;

