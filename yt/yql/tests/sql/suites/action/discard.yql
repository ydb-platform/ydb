/* syntax version 1 */
/* postgres can not */
define subquery $a() as
$_x = select 1;
discard
select ensure(1, true);
select 2;
$_y = select 2;
end define;

process $a();


define subquery $b() as
$f1 = ($row)->(<|a:1,b:$row.value|>);
$f2 = ($row)->(<|a:2,b:$row.value|>);
discard 
process plato.Input using $f1(TableRow());
process plato.Input using $f2(TableRow());
end define;

select * from $b() order by b limit 1;

define subquery $c() as
$f1 = ($key,$_)->(<|a:1,b:$key|>);
$f2 = ($key,$_)->(<|a:2,b:$key|>);
discard 
reduce plato.Input on key using $f1(TableRow());
reduce plato.Input on key using $f2(TableRow());
end define;

select * from $c() order by b limit 1;
