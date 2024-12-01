/* syntax version 1 */
values (1,2), (3,4);

select * from (values (1,2), (3,4));
select * from (values (1,2), (3,4)) as t(x,y);
select * from (values (1,2,3,4), (5,6,7,8)) as t(x,y,z);

select exists(values (1));
