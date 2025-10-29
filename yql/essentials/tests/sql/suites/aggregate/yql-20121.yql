$s = select x,y,sum(z) as cnt from (select 1 as x, 2 as y,10 as z) group by x,y;
$s2 = select x,y,sum(cnt) from $s group by x,y;
select x,count(*) from $s2 group by x;

