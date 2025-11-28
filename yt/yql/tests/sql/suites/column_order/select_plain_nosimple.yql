/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;
pragma DisableSimpleColumns;

select * from Input;
select * without key from Input;

select a.* from Input as a;
select a.* without key from Input as a;

select 1 as z, 2 as x, a.* from Input as a;
select 1 as z, 2 as x, a.* without key from Input as a;

select 1 as c, 2 as b, 3 as a;

