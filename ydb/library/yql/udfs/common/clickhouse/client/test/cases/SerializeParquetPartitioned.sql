/* syntax version 1 */
$callable = ($input)->{
    $serialize = YQL::Udf(AsAtom("ClickHouseClient.SerializeFormat"), Void(), TupleType(TupleType(TypeOf($input))), AsAtom('parquet{"keys":["utf"]}'));
    return Yql::Map($serialize($input), ($out)->(<|out:$out.0, key:$out.1|>));
};

$i1 = process Input
using $callable(TableRows());

select * from $i1
order by key
into result `Simple`;

$i2 = process (select (unsigned, signed, number, boolean) as many, utf from Input)
using $callable(TableRows());

select * from $i2
order by key
into result `Tuples`;

$i3 = process (select AGG_LIST(unsigned) as unsigned, AGG_LIST(signed) as signed, utf, AGG_LIST(number) as number, AGG_LIST(boolean) as boolean from Input group by utf)
using $callable(TableRows());

select * from $i3
order by key
into result `Lists`;

