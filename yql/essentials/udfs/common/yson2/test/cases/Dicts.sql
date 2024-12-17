$x = Yson::Parse("{a=1;a=2;b={c=3;d=4}}");
$no_strict = Yson::Options(false AS Strict);
select Yson::ConvertToDict($x) is null,
    DictLength(Yson::ConvertToDict($x)),
    DictKeys(Yson::ConvertToDict($x)),
    ListMap(DictPayloads(Yson::ConvertToDict($x)), ($i)->(Yson::ConvertToInt64($i, $no_strict))),
    ListMap(DictItems(Yson::ConvertToDict($x)),($p)->(($p.0,Yson::ConvertToInt64($p.1, $no_strict)))),
    DictContains(Yson::ConvertToDict($x),"a"),
    DictContains(Yson::ConvertToDict($x),"c"),
    Yson::ConvertToInt64(DictLookup(Yson::ConvertToDict($x),"a")),
    Yson::ConvertToInt64(DictLookup(Yson::ConvertToDict($x),"c")),
    DictKeys(Yson::ConvertToDict(Yson::ConvertToDict($x)["b"])),
    ListMap(DictPayloads(Yson::ConvertToDict(Yson::ConvertToDict($x)["b"])),($y)->(Yson::ConvertToInt64($y)))
