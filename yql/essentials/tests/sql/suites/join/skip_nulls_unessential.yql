pragma config.flags("OptimizerFlags", "EmitSkipNullOnPushdownUsingUnessential");

select *
from AS_TABLE([
    <|k1: 1|>,
    <|k1: 2|>,
    <|k1: 3|>,
    <|k1: 4|>,
    <|k1: NULL|>,
]) as t1
inner join AS_TABLE([
    <|k2: 2|>,
    <|k2: 3|>,
    <|k2: 4|>,
    <|k2: 5|>,
    <|k2: NULL|>,
]) as t2
on t1.k1 == t2.k2
where t1.k1 > 2
