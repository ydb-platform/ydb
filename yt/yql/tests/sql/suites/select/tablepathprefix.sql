/* postgres can not */
/* syntax version 1 */
use plato;
pragma TablePathPrefix = "//";

$input = "In" || "put";

select * from `Input` order by subkey;
select * from $input order by subkey;
