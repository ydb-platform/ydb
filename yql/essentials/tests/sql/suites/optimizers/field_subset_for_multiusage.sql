use plato;

select 
    key,
    count(1) as cnt,
    sum(cast(subkey as int32)) as sm
from concat(Input1, Input2)
where subkey in ("1", "2", "3", "4")
group by key
order by sm desc;

select
    count(1) as cnt,
    sum(cast(subkey as int32)) as sm
from concat(Input1, Input2);
