pragma warning("disable","4510");
$input = select 1 as key, 'foo' as value;

$f = ($rows)->{
    return Yql::Map($rows, ($row)->(<|key:$row.key + 1,value:$row.value|>));
};

PROCESS $input USING $f(TableRows());
