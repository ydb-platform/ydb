use plato;
pragma yt.UseNativeYtTypes;

insert into @foo
select 

    1 as a,
    Nothing(pgcstring) as i1,
    
    Just(Nothing(pgcstring)) as j1,
    Nothing(pgcstring?) as j2,
    
    Just(Just(Nothing(pgcstring))) as k1,
    Just(Nothing(pgcstring?)) as k2,
    Nothing(pgcstring??) as k3
    
;

commit;

insert into @bar
select t.a+1 as a,t.* without a from @foo as t;

commit;

select 
a,
i1,i1 is null as i1n,
j1,j1 is null as j1n,
j2,j2 is null as j2n,
k1,k1 is null as k1n,
k2,k2 is null as k2n,
k3,k3 is null as k3n
from @bar;






