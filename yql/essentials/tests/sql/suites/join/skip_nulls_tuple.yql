pragma config.flags("OptimizerFlags", "EmitSkipNullOnPushdownUsingUnessential");

select *
from AS_TABLE([
    <|k1: (1, 0)|>,
    <|k1: (2, 0)|>,
    <|k1: (3, 0)|>,
    <|k1: (4, 0)|>,
    <|k1: (NULL, 0)|>,
]) as t1
inner join AS_TABLE([
    <|k2: (2, 0)|>,
    <|k2: (3, 0)|>,
    <|k2: (4, 0)|>,
    <|k2: (5, 0)|>,
    <|k2: (NULL, 0)|>,
]) as t2
on t1.k1 == t2.k2
where t1.k1.0 > 2
