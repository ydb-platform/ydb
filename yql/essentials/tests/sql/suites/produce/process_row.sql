$input = select 1 as key, 'foo' as value;

$f = ($row)->{
    return <|key:$row.key + 1,value:$row.value|>;
};

PROCESS $input USING $f(TableRow());
