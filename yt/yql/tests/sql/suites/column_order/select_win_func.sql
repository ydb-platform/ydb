/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

select
  min(subkey) over (partition by key) as zz,
  row_number() over (order by key, subkey) as z,
  a.*
from Input as a
order by key, subkey;
