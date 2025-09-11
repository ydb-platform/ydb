/* syntax version 1 */
/* postgres can not */
use plato;

$src = select
  key,
  "ZZZ" || key as subkey,
  value,
from Input as u
assume order by key;

select * from $src where key < "075" or key > "075" order by key, subkey, value;

