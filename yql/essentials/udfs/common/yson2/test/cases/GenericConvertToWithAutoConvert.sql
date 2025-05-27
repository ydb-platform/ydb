$ac = Yson::Options(true AS AutoConvert);

SELECT
    Yson::ConvertTo(Yson::Parse(Yson("yes")), Bool, $ac) AS `bool`,
    Yson::ConvertTo(Yson::Parse(Yson("no")), Int64, $ac) AS `int`,
    Yson::ConvertTo(Yson::Parse(Yson("123.7")), Uint8, $ac) AS `uint`,
    Yson::ConvertTo(Yson::Parse(Yson(@@"1.23"@@)), Double?, $ac) AS optional_double,
    Yson::ConvertTo(Yson::Parse(Yson("many")), Int32?, $ac) AS empty_int,
    Yson::ConvertTo(Yson::Parse(Yson("1.23")), String, $ac) AS `string`,
    Yson::ConvertTo(Yson::Parse(Yson("0u")), Utf8, $ac) AS `utf8`,
    Yson::ConvertTo(Yson::Parse(Yson(@@[1;2;3;7.7;"8";"9.0"]@@)), List<Int64>, $ac) AS int_list,
    Yson::ConvertTo(Yson::Parse(Yson("[[1;2];[3;#];5;#]")), List<List<Int64?>>, $ac) AS nested_list,
    Yson::ConvertTo(Yson::Parse(Yson("{foo=1;bar=2.0;xxx=#}")), Dict<String,Int64>, $ac) AS int_dict,
    Yson::ConvertTo(Yson::Parse(Yson("[%false;1;\"foo\";[1;2]]")), Tuple<Bool,Int8,String?,List<Int64>,Yson,Json>, $ac) AS `tuple`,
    Yson::ConvertTo(Yson::Parse(Yson("{a=%false;b=1;c=foo;d=[1;2];e=[1.0;bar]}")), Struct<a:Bool,b:Int8,c:String?,d:List<Int64>,e:Tuple<Double,String>,x:Tuple<Double?,String?>,y:Int16?,z:List<Int8>>, $ac) AS `struct`;
