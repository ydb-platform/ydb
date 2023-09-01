$node = Yson::Parse(@@<x="y">{abc=123;}@@);
$data = Yson::YPath($node, "/abc");
$attrs = Yson::YPath($node, "/@");
$miss = Yson::YPath($node, "/def");

SELECT
    Yson::ConvertToInt64($data) AS data,
    Yson::ConvertToStringDict($attrs) AS attrs,
    Yson::SerializePretty($miss) AS miss,
    Yson::YPathInt64($node, "/abc") AS num,
    Yson::YPathString($node, "/@/x") AS str_attr,
    Yson::YPathBool($node, "/@/mis") AS miss_attr,
    Yson::YPathString($node, "/abc", Yson::Options(false as Strict)) AS bad_conv;
