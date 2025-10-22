/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

define subquery $select_star($table) as
  select * without subkey from $table;
end define;

select * from $select_star("Input");

