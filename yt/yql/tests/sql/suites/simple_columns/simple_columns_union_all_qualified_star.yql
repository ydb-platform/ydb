/* postgres can not */
use plato;
PRAGMA simplecolumns;

insert into @A (key, value) values
('x', 1),
('y', 2);

insert into @B (key, value) values
('y', 3),
('z', 4);
commit;

select A.* from @A AS A LEFT ONLY JOIN @B AS B ON A.key = B.key
UNION ALL
select B.* from @A AS A RIGHT ONLY JOIN @B AS B ON A.key = B.key


