use plato;
pragma yt.UseNativeYtTypes;

insert into @foo
select 1 as x,Nothing(pgcstring) as i1,Just(Nothing(pgcstring)) as i2,Just(Just(Nothing(pgcstring))) as i3;

commit;

insert into @bar
select x+1 as x,i1,i2,i3,i1 is null as i1n,i2 is null as i2n,i3 is null as i3n from @foo;
commit;

select * from @bar;

