$n = Nothing(pgint4);
$n2 = Nothing(pgint4) + 0p;
$a = 1p;
$b = 2p;
$b2 = 1p + 1p;
select
    AsStruct(Yql::AggrLess($a, $b) as x1, Yql::AggrLess($b, $a) as x2, Yql::AggrLess($b, $b2) as x3,
        Yql::AggrLess($a, $n) as y1, Yql::AggrLess($n, $a) as y2, Yql::AggrLess($n, $n2) as y3),
    AsStruct(Yql::AggrLessOrEqual($a, $b) as x1, Yql::AggrLessOrEqual($b, $a) as x2, Yql::AggrLessOrEqual($b, $b2) as x3,
        Yql::AggrLessOrEqual($a, $n) as y1, Yql::AggrLessOrEqual($n, $a) as y2, Yql::AggrLessOrEqual($n, $n2) as y3),      
    AsStruct(Yql::AggrGreater($a, $b) as x1, Yql::AggrGreater($b, $a) as x2, Yql::AggrGreater($b, $b2) as x3,
        Yql::AggrGreater($a, $n) as y1, Yql::AggrGreater($n, $a) as y2, Yql::AggrGreater($n, $n2) as y3),
    AsStruct(Yql::AggrGreaterOrEqual($a, $b) as x1, Yql::AggrGreaterOrEqual($b, $a) as x2, Yql::AggrGreaterOrEqual($b, $b2) as x3,
        Yql::AggrGreaterOrEqual($a, $n) as y1, Yql::AggrGreaterOrEqual($n, $a) as y2, Yql::AggrGreaterOrEqual($n, $n2) as y3),      
