/* syntax version 1 */
USE plato;

pragma SimpleColumns;
pragma CoalesceJoinKeysOnQualifiedAll;

select 
    b.* without b.x
from (select * from (select AsList(1, 2, 3) as x, AsList(1, 2) as y) flatten by (x, y)) as a
join (select * from (select AsList(1, 2, 3) as x, AsList(2, 3) as y) flatten by (x, y)) as b
on a.x == b.x and a.y == b.y;

select 
    * without b.x
from (select * from (select AsList(1, 2, 3) as x, AsList(1, 2) as y) flatten by (x, y)) as a
join (select * from (select AsList(1, 2, 3) as x, AsList(2, 3) as y) flatten by (x, y)) as b
on a.x == b.x and a.y == b.y

