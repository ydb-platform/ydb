pragma CompactNamedExprs;

$src = select 1;

define subquery $sub1() as
  select * from $src;
end define;

$foo = 1+2;

define subquery $sub2($sub, $extra) as
  select a.*, $extra as extra, $foo as another from $sub() as a
end define;

select * from $sub1();
select * from $sub2($sub1, 1);
select * from $sub2($sub1, "aaa");

define action $hello_world($sub, $name, $suffix?) as
    $name = $name ?? ($suffix ?? "world");
    select "Hello, " || $name || "!" from $sub();
end define;

do empty_action();
do $hello_world($sub1, null);
do $hello_world($sub1, null, "John");
do $hello_world($sub1, null, "Earth");
