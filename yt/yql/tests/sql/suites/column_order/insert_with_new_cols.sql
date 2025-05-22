/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

insert into Output with truncate 
select a.*
, count(key) over (partition by subkey) as cnt
from Input as a 
