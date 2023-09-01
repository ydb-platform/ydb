/* syntax version 1 */
$callable = ($input)->{
    $serialize = YQL::Udf(AsAtom("ClickHouseClient.SerializeFormat"), Void(), TupleType(TupleType(TypeOf($input))), AsAtom("json_each_row"));
    return Yql::Map($serialize($input), ($out)->(<|out:$out|>));
};

process Input
using $callable(TableRows())
into result `Simple`;

process (select (unsigned, signed, utf, number, boolean) as many from Input)
using $callable(TableRows())
into result `Tuples`;

process (select AGG_LIST(unsigned) as unsigned, AGG_LIST(signed) as signed, AGG_LIST(utf) as utf, AGG_LIST(number) as number, AGG_LIST(boolean) as boolean from Input)
using $callable(TableRows())
into result `Lists`;

