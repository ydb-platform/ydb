/* syntax version 1 */
$x = Yson::Parse("{}");
select Yson::ConvertToDict($x) is null,
    DictLength(Yson::ConvertToDict($x)),
    DictKeys(Yson::ConvertToDict($x)),  
    ListMap(DictPayloads(Yson::ConvertToDict($x)),($y)->(Yson::ConvertToInt64($y))),
    ListMap(DictItems(Yson::ConvertToDict($x)),($p)->(($p.0,Yson::ConvertToInt64($p.1)))),
    DictContains(Yson::ConvertToDict($x),"a"),
    Yson::ConvertToInt64(DictLookup(Yson::ConvertToDict($x),"a"));
