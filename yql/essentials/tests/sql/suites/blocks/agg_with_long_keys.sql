pragma config.flags("PeepholeFlags","UseAggPhases");

$src = ListMap(ListFromRange(cast (0 as Int64), cast(500 as Int64)), ($keyVal) -> {
    return <|
        k1: $keyVal,
        k2: $keyVal + 1,
        k3: $keyVal + 2,
        v: $keyVal
    |>;
});

select k1, k2, k3, sum(v) as s from as_table($src) group by k1, k2, k3 order by k1, k2, k3, s;

