use plato;

pragma yt.MapJoinLimit="10G";
pragma yt.EnableFuseMapToMapReduce = "true";

select t2.value
from Input
    cross join (VALUES (1, 1998), (2, 1998), (3, 1999)) as t1(key1, year)
    cross join (VALUES (4, "A"), (5, "B"), (6, "C")) as t2(key2, value)
where Input.key1 = t1.key1
    and t2.key2 = Input.key2
    and t1.year = 1998
group by t2.value;
