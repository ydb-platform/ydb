$n = Nothing(pgint4);
$n2 = Nothing(pgint4) + 0p;
$a = 1p;
$b = 2p;
$b2 = 1p + 1p;

SELECT
    AsStruct(
        Yql::AggrLess($a, $b) AS x1, Yql::AggrLess($b, $a) AS x2, Yql::AggrLess($b, $b2) AS x3,
        Yql::AggrLess($a, $n) AS y1, Yql::AggrLess($n, $a) AS y2, Yql::AggrLess($n, $n2) AS y3
    ),
    AsStruct(
        Yql::AggrLessOrEqual($a, $b) AS x1, Yql::AggrLessOrEqual($b, $a) AS x2, Yql::AggrLessOrEqual($b, $b2) AS x3,
        Yql::AggrLessOrEqual($a, $n) AS y1, Yql::AggrLessOrEqual($n, $a) AS y2, Yql::AggrLessOrEqual($n, $n2) AS y3
    ),
    AsStruct(
        Yql::AggrGreater($a, $b) AS x1, Yql::AggrGreater($b, $a) AS x2, Yql::AggrGreater($b, $b2) AS x3,
        Yql::AggrGreater($a, $n) AS y1, Yql::AggrGreater($n, $a) AS y2, Yql::AggrGreater($n, $n2) AS y3
    ),
    AsStruct(
        Yql::AggrGreaterOrEqual($a, $b) AS x1, Yql::AggrGreaterOrEqual($b, $a) AS x2, Yql::AggrGreaterOrEqual($b, $b2) AS x3,
        Yql::AggrGreaterOrEqual($a, $n) AS y1, Yql::AggrGreaterOrEqual($n, $a) AS y2, Yql::AggrGreaterOrEqual($n, $n2) AS y3
    ),
;
