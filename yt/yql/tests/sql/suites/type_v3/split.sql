USE plato;
PRAGMA yt.MaxInputTables="2";

insert into Output
select * from concat(Input1, Input2, Input3);

