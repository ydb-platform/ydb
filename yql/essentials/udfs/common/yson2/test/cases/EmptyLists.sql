/* syntax version 1 */
$x = Yson::Parse("[]");
select Yson::ConvertToList($x) is null,
    ListLength(Yson::ConvertToList($x)),
    ListMap(Yson::ConvertToList($x), ($y)->(Yson::ConvertToInt64($y)));
