/* postgres can not */
use plato;
select "input1-value1" as src, key, WeakField(value1, "String", "funny") as ozer from Input1
union all
select "input2-value1" as src, key, WeakField(value1, "String", "funny") as ozer from Input2
union all
select "input1-value2" as src, key, WeakField(value2, "String") as ozer from Input1
union all
select "input2-value2" as src, key, WeakField(value2, "String") as ozer from Input2
