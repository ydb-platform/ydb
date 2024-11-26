/* syntax version 1 */
$bool = Yson::Parse("true");
$number = Yson::Parse("123");
$string = Yson::Parse("\"123\"");
$number_list = Yson::Parse("[1;2;3]");
$string_list = Yson::Parse("[\"a\";\"b\";\"c\"]");
$yson_list = Yson::Parse("[123;{a=1;b=2;c=3};{a=4;b=5;c=6}]");
$number_dict = Yson::Parse("{a=1;b=2;c=3}");
$string_dict = Yson::Parse("{a=\"aaa\";b=\"bbb\";c=\"ccc\"}");
$yson_dict = Yson::Parse("{a=123;b=\"bbb\";c=[\"ccc\";\"ddd\"]}");
$options = Yson::Options(true AS Strict);
$no_strict = Yson::Options(false AS Strict);

SELECT
    Yson::ConvertToBool($bool, $options) AS `bool`,
    Yson::ConvertToInt64($number, $options) AS `int`,
    Yson::ConvertToUint64($number, $options) AS `uint`,
    Yson::ConvertToDouble($number, $options) AS `double`,
    Yson::ConvertToString($string, $options) AS `string`,
    Yson::ConvertToInt64($string, $no_strict) AS incorrect,
    Yson::ConvertToUint64List($number_list) AS number_list,
    Yson::ConvertToStringList($string_list) AS string_list,
    ListMap(
        Yson::ConvertToList($yson_list),
        ($item) -> { return Yson::SerializeJson($item); }
    ) AS yson_list,
    Yson::ConvertToStringList($number_list, $no_strict) AS incorrect_list,
    Yson::ConvertToInt64Dict($number_dict) AS number_dict,
    Yson::ConvertToStringDict($string_dict) AS string_dict,
    Yson::SerializeJson(
        Yson::ConvertToDict($yson_dict)["c"]
    ) AS yson_dict,
    Yson::ConvertToBoolDict($number_dict, $no_strict) AS incorrect_dict;

