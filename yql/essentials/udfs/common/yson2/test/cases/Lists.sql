/* syntax version 1 */
$x = Yson::Parse("[1;2;[3;4]]");
$no_strict = Yson::Options(false AS Strict);
select Yson::ConvertToList($x) is null,
    ListLength(Yson::ConvertToList($x)),
    ListMap(Yson::ConvertToList($x), ($i)->(Yson::ConvertToInt64($i,$no_strict))),
    ListMap(Yson::ConvertToList($x), ($x)->(ListMap(Yson::ConvertToList($x, Yson::Options(false AS Strict)), Yson::ConvertToInt64)));

$int_and_str = Yson(@@[123;"456"]@@);
SELECT ListMap(Yson::ConvertToList($int_and_str), Yson::ConvertToString), ListMap(Yson::ConvertToList($int_and_str), Yson::ConvertToInt64);
