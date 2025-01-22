/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;
pragma yt.PoolTrees='physical,cloud';

insert into Input
select key, subkey, value from Input;
