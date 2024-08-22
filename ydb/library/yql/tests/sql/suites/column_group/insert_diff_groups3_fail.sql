USE plato;

pragma yt.ColumnGroupMode="perusage";

insert into Input with column_groups="{a=#}"
select * from Input where a != "";
