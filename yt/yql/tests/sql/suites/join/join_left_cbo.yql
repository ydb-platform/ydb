use plato;
pragma warning("disable", "8001"); -- CBO_MISSING_TABLE_STATS

pragma CostBasedOptimizer="PG";

select i1.value, i2.value
from Input1 as i1
left join Input2 as i2 on i1.key=i2.key
order by i1.value, i2.value;
