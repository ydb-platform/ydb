/* custom error:Insert with different "column_groups" to existing table is not allowed*/
USE plato;

pragma yt.ColumnGroupMode="perusage";

insert into Output with column_groups="{a=#}"
select * from Input where a != "";
