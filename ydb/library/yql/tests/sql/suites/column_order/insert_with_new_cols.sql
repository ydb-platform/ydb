/* postgres can not */
/* syntax version 1 */
/* skip double format */
use plato;
pragma OrderedColumns;

insert into Output with truncate 
select a.*
, count(key) over (partition by subkey) as cnt
from Input as a 
