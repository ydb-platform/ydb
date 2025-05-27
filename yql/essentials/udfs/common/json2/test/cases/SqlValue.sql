/* syntax version 1 */

-- Plain cases
$path = Json2::CompilePath("strict $.x");
$empty_object = CAST("{}" as Json);
$empty_array = CAST("[]" as Json);
$null_key = CAST(@@{
    "x": null
}@@ as Json);

$string_json = CAST(@@{
    "x": "some string value"
}@@ as Json);
SELECT
    Json2::SqlValueUtf8($string_json, $path, AsDict()),
    Json2::SqlValueUtf8($empty_object, $path, AsDict()),
    Json2::SqlValueUtf8($empty_array, $path, AsDict()),
    Json2::SqlValueUtf8($null_key, $path, AsDict()),
    Json2::SqlValueUtf8(NULL, $path, AsDict());

$int64_json = CAST(@@{
    "x": 2856
}@@ as Json);
SELECT
    Json2::SqlValueInt64($int64_json, $path, AsDict()),
    Json2::SqlValueInt64($empty_object, $path, AsDict()),
    Json2::SqlValueInt64($empty_array, $path, AsDict()),
    Json2::SqlValueInt64($null_key, $path, AsDict()),
    Json2::SqlValueInt64(NULL, $path, AsDict());

$double_json = CAST(@@{
    "x": 2.71828
}@@ as Json);
SELECT
    Json2::SqlValueNumber($double_json, $path, AsDict()),
    Json2::SqlValueNumber($empty_object, $path, AsDict()),
    Json2::SqlValueNumber($empty_array, $path, AsDict()),
    Json2::SqlValueNumber($null_key, $path, AsDict()),
    Json2::SqlValueNumber(NULL, $path, AsDict());

$bool_json = CAST(@@{
    "x": true
}@@ as Json);
SELECT
    Json2::SqlValueBool($bool_json, $path, AsDict()),
    Json2::SqlValueBool($empty_object, $path, AsDict()),
    Json2::SqlValueBool($empty_array, $path, AsDict()),
    Json2::SqlValueBool($null_key, $path, AsDict()),
    Json2::SqlValueBool(NULL, $path, AsDict());

-- Convert cases
SELECT
    Json2::SqlValueConvertToUtf8($string_json, $path, AsDict()),
    Json2::SqlValueConvertToUtf8($int64_json, $path, AsDict()),
    Json2::SqlValueConvertToUtf8($double_json, $path, AsDict()),
    Json2::SqlValueConvertToUtf8($bool_json, $path, AsDict()),
    -- NOTE: Here SQL null must be returned, not "null" string
    Json2::SqlValueConvertToUtf8($null_key, $path, AsDict()),
    Json2::SqlValueConvertToUtf8(NULL, $path, AsDict());

-- Error cases
SELECT
    Json2::SqlValueUtf8($int64_json, $path, AsDict()),
    Json2::SqlValueUtf8($double_json, $path, AsDict()),
    Json2::SqlValueUtf8($bool_json, $path, AsDict());

SELECT
    Json2::SqlValueInt64($string_json, $path, AsDict()),
    Json2::SqlValueInt64($double_json, $path, AsDict()),
    Json2::SqlValueInt64($bool_json, $path, AsDict());

SELECT
    Json2::SqlValueNumber($string_json, $path, AsDict()),
    -- NOTE: Here int64 is automatically converted to double as it is possible without precision loss
    Json2::SqlValueNumber($int64_json, $path, AsDict()),
    Json2::SqlValueNumber($bool_json, $path, AsDict());

SELECT
    Json2::SqlValueBool($string_json, $path, AsDict()),
    Json2::SqlValueBool($int64_json, $path, AsDict()),
    Json2::SqlValueBool($double_json, $path, AsDict());

$nested_object_json = CAST(@@{
    "x": {
        "a": 1
    }
}@@ as Json);

$nested_array_json = CAST(@@{
    "x": [29, 32, "some string"]
}@@ as Json);

SELECT
    Json2::SqlValueBool($nested_object_json, $path, AsDict()),
    Json2::SqlValueBool($nested_array_json, $path, AsDict());