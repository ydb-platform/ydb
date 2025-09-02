/* syntax version 1 */
use plato;

insert into @Input1
select '' as k1, '' as v1, '' as u1 limit 0;
commit;

select 

v3

from @Input1 as a 
join Input2 as b on (a.k1 = b.k2)
right join Input3 as c on (a.k1 = c.k3)
order by v3;
