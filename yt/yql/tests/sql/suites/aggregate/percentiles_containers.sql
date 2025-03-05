select
    key,
    median(val) as med,
    percentile(val, AsTuple(0.2, 0.4, 0.6)) as ptuple,
    percentile(val, AsStruct(0.2 as p20, 0.4 as p40, 0.6 as p60)) as pstruct,
    percentile(val, AsList(0.2, 0.4, 0.6)) as plist,
from (select key, cast(value as int) as val from plato.Input)
group by key
order by key;

select
    median(val) as med,
    percentile(val, AsTuple(0.2, 0.4, 0.6)) as ptuple,
    percentile(val, AsStruct(0.2 as p20, 0.4 as p40, 0.6 as p60)) as pstruct,
    percentile(val, AsList(0.2, 0.4, 0.6)) as plist,
from (select key, cast(value as int) as val from plato.Input)
