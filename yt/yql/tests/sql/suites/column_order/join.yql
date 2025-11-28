/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

$foo = select 1 as sk, "150" as key, 2 as v;

select * from $foo as b join Input as a using(key);
select a.* from $foo as b join Input as a using(key);
select b.* from $foo as b join Input as a using(key);
select a.*, b.* from $foo as b join Input as a using(key);
