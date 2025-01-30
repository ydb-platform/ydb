pragma warning("disable","4510");
$init = ListCreate(Struct<n:Int32>);
$transform = ($value)->{
    return ListMap(ListFilter($value, ($r)->($r.n<5)), ($r)->(<|n:$r.n + 1|>));
};

select * from AS_TABLE(Yql::PgIterateAll($init,$transform)) order by n;

$init = [<|n:1|>];
$transform = ($value)->{
    return ListMap(ListFilter($value, ($r)->($r.n<5)), ($r)->(<|n:$r.n + 1|>));
};

select * from AS_TABLE(Yql::PgIterateAll($init,$transform)) order by n;

$init = [<|n:1|>, <|n:1|>, <|n:2|>];
$transform = ($value)->{
    return ListFlatMap($value, ($_r)->([<|n:1|>,<|n:2|>,<|n:2|>]));
};

select * from AS_TABLE(Yql::PgIterate($init,$transform)) order by n;

