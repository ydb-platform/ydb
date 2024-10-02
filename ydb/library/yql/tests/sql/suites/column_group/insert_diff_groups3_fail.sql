USE plato;

pragma yt.ColumnGroupMode="perusage";

insert into Output with column_groups="{a=#}"
select * from Input where a != "";
