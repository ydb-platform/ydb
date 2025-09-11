/* postgres can not */
/* multirun can not */

insert into plato.Output with truncate
select * from plato.Input order by key;

insert into plato.Output
select * from plato.Input;

commit;
----------------------------------------

insert into plato.Output with truncate
select * from plato.Input;

insert into plato.Output
select * from plato.Input order by key;

commit;
----------------------------------------

insert into plato.Output with truncate
select * from plato.Input order by key, subkey;

insert into plato.Output
select * from plato.Input order by key, subkey;

commit;
----------------------------------------

insert into plato.Output with truncate
select * from plato.Input order by key;

insert into plato.Output
select * from plato.Input order by key desc;

commit;
----------------------------------------

insert into plato.Output with truncate
select * from plato.Input order by key;

insert into plato.Output
select * from plato.Input order by key || subkey;

commit;
----------------------------------------

insert into plato.Output with truncate
select * from plato.Input order by key desc;

insert into plato.Output
select * from plato.Input order by key desc;

commit;
----------------------------------------

insert into plato.Output with truncate
select * from plato.Input order by key || subkey;

insert into plato.Output
select * from plato.Input order by key || subkey;

