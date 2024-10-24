USE plato;

pragma yt.ColumnGroupMode="perusage";

$s1 = select * from Input where a != "";
$s2 = select * from Input where a > "a1";

insert into Output
select * from $s1;

commit;

insert into Output with column_groups="{a=#}"
select * from $s2;
