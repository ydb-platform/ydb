/* multirun can not */

insert into plato.Output1
select * from plato.Input order by key;

insert into plato.Output2
select * from plato.Input order by key, subkey;

insert into plato.Output3
select * from plato.Input order by key, subkey, value;

insert into plato.Output4
select * from plato.Input;

insert into plato.Output5
select * from plato.Input order by subkey;

insert into plato.Output6
select * from plato.Input order by key desc;

insert into plato.Output7
select * from plato.Input order by key desc, subkey;

insert into plato.Output8
select * from plato.Input order by key || subkey;
