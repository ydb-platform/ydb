--pragma config.flags("LLVM_OFF");

$f = ($value) -> {
   return <|
       "plain":$value,
       "opt_1+":just($value),
       "opt_1-":nothing(optionaltype(typeof($value))),
       "opt_2++":just(just($value)),
       "opt_2+-":just(nothing(optionaltype(typeof($value)))),
       "opt_2--":nothing(optionaltype(optionaltype(typeof($value)))),
       "tuple":($value,),
    |>;
};

$data = <|
    "bool": true,
    "int8": Int8('1'),
    "uint8": Uint8('2'),
    "int16": Int16('3'),
    "uint16": Uint16('4'),
    "int32": Int32('5'),
    "uint32": Uint32('6'),    
    "int64": Int64('7'),
    "uint64": Uint64('8'),
    "float": Float('9'),
    "double": Double('10'),
    "decimal": Decimal('11.3',5,1),
    "string": "a",
    "utf8": "b"u,
    "yson": "{}"y,
    "json": "[]"j,
    "jsondocument": jsondocument('{"a":1}'),
    "uuid": uuid("487120b0-a07f-45be-8d8d-77f727a097a2"),
    "dynumber": dynumber("12.4"),
    "date": date("2000-01-02"),
    "datetime": datetime("2000-01-02T03:04:05Z"),
    "timestamp": timestamp("2000-01-02T03:04:05.6789012Z"),
    "interval": interval("P1D"),
    "tzdate": tzdate("2000-01-02,Europe/Moscow"),
    "tzdatetime": tzdatetime("2000-01-02T03:04:05,Europe/Moscow"),
    "tztimestamp": tztimestamp("2000-01-02T03:04:05.6789012,Europe/Moscow"),
    "date32": date32("1900-01-02"),
    "datetime64": datetime64("1900-01-02T03:04:05Z"),
    "timestamp64": timestamp64("1900-01-02T03:04:05.6789012Z"),
    "interval64": interval64("P1D"),
    "tzdate32": tzdate32("1900-01-02,Europe/Moscow"),
    "tzdatetime64": tzdatetime64("1900-01-02T03:04:05,Europe/Moscow"),
    "tztimestamp64": tztimestamp64("1900-01-02T03:04:05.6789012,Europe/Moscow"),    
|>;

evaluate for $name in StructMembers($data) do begin 
select * from (
    select $f($data.$name)
) flatten columns into result $name;
end do;

