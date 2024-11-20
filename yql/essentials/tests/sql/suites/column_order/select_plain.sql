/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

select * from Input order by subkey;
select * without key from Input order by subkey;

select a.* from Input as a order by a.subkey;
select a.* without key from Input as a order by a.subkey;

select 1 as z, 2 as x, a.* from Input as a order by a.subkey;
select 1 as z, 2 as x, a.* without key from Input as a order by a.subkey;

select 1 as c, 2 as b, 3 as a;

