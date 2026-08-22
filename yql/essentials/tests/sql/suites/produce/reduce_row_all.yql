pragma warning("disable","4510");
$input = select 1 as key, 'foo' as value;

$f = ($stream)->{
    return Yql::Map($stream, ($p)->(<|key:$p.0,len:ListLength(Yql::Collect($p.1))|>));
};

REDUCE $input ON key USING ALL $f(TableRow());
