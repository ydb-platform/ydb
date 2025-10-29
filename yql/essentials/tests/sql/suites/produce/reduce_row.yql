pragma warning("disable","4510");
$input = select 1 as key, 'foo' as value;

$f = ($key, $stream)->{
    return <|key:$key,len:ListLength(Yql::Collect($stream))|>
};

REDUCE $input ON key USING $f(TableRow());
