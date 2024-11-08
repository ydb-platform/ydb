$ns = Yson::Options(false AS Strict);

SELECT
    Yson::ConvertTo(Yson::Parse(Yson("yes")), Bool?, $ns) AS `bool`,
    Yson::ConvertTo(Yson::Parse(Yson("no")), Int64?, $ns) AS `int`,
    Yson::ConvertTo(Yson::Parse(Yson("123.7")), Uint8?, $ns) AS `uint`,
    Yson::ConvertTo(Yson::Parse(Yson(@@"1.23"@@)), Double?, $ns) AS optional_double,
    Yson::ConvertTo(Yson::Parse(Yson("many")), Int32?, $ns) AS empty_int,
    Yson::ConvertTo(Yson::Parse(Yson("1.23")), String?, $ns) AS `string`,
    Yson::ConvertTo(Yson::Parse(Yson("0u")), Utf8?, $ns) AS `utf8`,
    Yson::ConvertTo(Yson::Parse(Yson(@@[1;2;3;7.7;"8";"9.0"]@@)), List<Int64>, $ns) AS int_list,
    Yson::ConvertTo(Yson::Parse(Yson("[[1;2];[3;#];5;#]")), List<List<Int64?>>, $ns) AS nested_list,
    Yson::ConvertTo(Yson::Parse(Yson("{foo=1;bar=2.0;xxx=#}")), Dict<String,Int64>, $ns) AS int_dict,
    Yson::ConvertTo(Yson::Parse(Yson("[%false;1;42;[1;2;3.3]]")), Tuple<Bool,Int8,String?,List<Int64>>, $ns) AS `tuple`,
    Yson::ConvertTo(Yson::Parse(Yson("{a=%false;b=1;c=foo;d=[1;2];e=[1.0;bar]}")), Struct<a:Bool,b:Int8,c:String?,d:List<Int64>,e:Tuple<Double,String>,y:Int16?>, $ns) AS `struct`;
