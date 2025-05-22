pragma warning("disable","4510");
$input = select 1 as key, 'foo' as value;

$r = REDUCE $input ON key USING SimpleUdf::GenericAsStruct(TableRow().value);

select arg_0, Yql::Collect(arg_1) from $r;
