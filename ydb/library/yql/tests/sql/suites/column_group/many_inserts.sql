USE plato;

pragma yt.ColumnGroupMode="perusage";

$s1 = select * from Input where a != "";
$s2 = select * from Input where a > "a1";

insert into @a with column_groups="{a=#}"
select * from $s1;

insert into @b
select * from $s1;

insert into @c
select * from $s1;

insert into Output with column_groups="{a=#}"
select * from $s1;

commit;

insert into @a with column_groups="{a=#}"
select * from $s2;

insert into @b
select * from $s2;

insert into @c with column_groups="{default=#}"
select * from $s2;

insert into Output with column_groups="{a=#}"
select * from $s2;
