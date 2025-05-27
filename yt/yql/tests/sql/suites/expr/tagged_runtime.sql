/* postgres can not */
use plato;
insert into @tmp
select Just((
    AsTagged(1,"A"), 
    AsTagged(just(2),"B"), 
    AsTagged(null,"C"),
    AsTagged(Nothing(Int32?),"D"),
    AsTagged(Nothing(pgint4?),"E")
    )) as x;
commit;
select x.0, x.1, x.2, x.3, x.4 from @tmp
