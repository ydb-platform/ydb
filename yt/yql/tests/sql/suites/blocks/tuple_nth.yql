USE plato;

insert into @tmp
SELECT
    key,
    (subkey,key) as a,
    (1,key) as b,
    Just((subkey,key)) as c,
    Just((Just(subkey),key)) as d,
    Nothing(Tuple<Int32,Int32>?) as e,
    Nothing(Tuple<Int32?,Int32>?) as f,
FROM Input;

commit;

select a.0,a.1,b.0,b.1,c.0,c.1,d.0,d.1,e.0,e.1,f.0,f.1 from @tmp;
