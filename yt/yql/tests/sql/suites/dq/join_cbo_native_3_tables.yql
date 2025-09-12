use plato;

pragma CostBasedOptimizer="native";

select i1.value, i2.value, i3.value, i4.value 
from Input1 as i1
join Input2 as i2 on i1.key=i2.key
join Input3 as i3 on i1.key=i3.key
join Input4 as i4 on i1.key=i4.key
order by i1.value, i2.value, i3.value, i4.value;
