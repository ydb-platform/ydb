use plato;

insert into @a select 1 as t;

commit;

select * from @a;
select * from a;
