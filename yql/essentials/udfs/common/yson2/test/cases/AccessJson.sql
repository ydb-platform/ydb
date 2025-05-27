PRAGMA yson.DisableStrict;
$yson = cast(@@{a="привет"}@@ as yson);
$yson_node = Yson::Parse($yson);

select Yson::ConvertToString($yson.a);
select Yson::ConvertToString($yson_node.a);

$json = cast(@@{"a":"привет"}@@ as json);
$json_node = Yson::ParseJson($json);

select Yson::ConvertToString($json.a);
select Yson::ConvertToString($json_node.a);
