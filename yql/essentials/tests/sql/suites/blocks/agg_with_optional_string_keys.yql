pragma config.flags("PeepholeFlags","UseAggPhases");

$src = ListMap(ListFromRange(cast (0 as Int64), cast(500 as Int64)), ($x) -> {
    return <|
        k: if ($x % 10 == 0, NULL, cast ($x as String)),
        v: $x
    |>;
});

select k, sum(v) as s from as_table($src) group by k order by k, s;

