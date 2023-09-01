/* syntax version 1 */
$f = ($name)->{
    $type = Unwrap(ClickHouseClient::ToYqlType($name,"UTC"u), $name);
    return ($type, FormatType(ParseTypeHandle(CAST($type AS String))));
};

select
    $f("Int8"u),
    $f("UInt8"u),
    $f("Int16"u),
    $f("UInt16"u),
    $f("Int32"u),
    $f("UInt32"u),
    $f("Int64"u),
    $f("UInt64"u),
    $f("Float32"u),
    $f("Float64"u),
    $f("String"u),
    $f("FixedString(10)"u),
    $f("Date"u),
    $f("Datetime"u),
    $f("UUID"u),
    $f("Nullable(Int8)"u),
    $f("Nullable(Nothing)"u),
    $f("Array(Int8)"u),
    $f("Array(Nothing)"u),
    $f("Tuple(Int32,String)"u),
    $f("Enum8('a'=1)"u),
    $f("Enum16('b'=1000)"u)
;
