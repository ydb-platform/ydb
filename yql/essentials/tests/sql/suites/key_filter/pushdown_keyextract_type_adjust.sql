/* syntax version 1 */
/* postgres can not */
use plato;

$src = select
  Just(key) as key,
  "ZZZ" || subkey as subkey,
  value
from Input as u
assume order by key, subkey, value;

select * from $src where key > "023" and key < "150" order by key;

