--!syntax_v1

$from_jd = JSON_QUERY(JsonDocument("[1, 2, 3, 4]"), "$");
$from_json = JSON_QUERY(Json(@@{
    "key": "value"
}@@), "$");

SELECT
    FormatType(TypeOf($from_jd)),
    $from_jd,
    FormatType(TypeOf($from_json)),
    $from_json;