use plato;

insert into Output select sum(cast(key as int32)) from Input1;
insert into Output select sum(cast(key as int32)) from Input2;
insert into Output select sum(cast(key as int32)) from Input3;
