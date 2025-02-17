/* syntax version 1 */
use plato;

$table = "In" || "put";

select *
from $table
with schema Struct<a:Int64?>;
