/* syntax version 1 */
/* postgres can not */

-- Basic cases
$bool_json = CAST(@@{
    "key": true
}@@ as Json);
SELECT
    JSON_VALUE($bool_json, "strict $.key"), -- defaults to RETURNING Utf8 with cast to string value
    JSON_VALUE($bool_json, "strict $.key" RETURNING Bool);

$string_json = CAST(@@{
    "key": "string"
}@@ as Json);
SELECT
    JSON_VALUE($string_json, "strict $.key"), -- defaults to RETURNING Utf8 with cast to string value
    JSON_VALUE($string_json, "strict $.key" RETURNING Utf8);

$int64_json = CAST(@@{
    "key": 123
}@@ as Json);
SELECT
    JSON_VALUE($int64_json, "strict $.key"), -- defaults to RETURNING Utf8 with cast to string value
    JSON_VALUE($int64_json, "strict $.key" RETURNING Int64);

$double_json = CAST(@@{
    "key": 123.456
}@@ as Json);
SELECT
    JSON_VALUE($double_json, "strict $.key"), -- defaults to RETURNING Utf8 with cast to string value
    JSON_VALUE($double_json, "strict $.key" RETURNING Double);

-- Casting of result value to different types
-- From integer to various numeric types
SELECT
    JSON_VALUE($int64_json, "strict $.key" RETURNING Int8),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Uint8),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Int16),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Uint16),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Int32),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Uint32),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Int64),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Uint64),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Double),
    JSON_VALUE($int64_json, "strict $.key" RETURNING Float);

-- From double to Double and Float
SELECT
    JSON_VALUE($double_json, "strict $.key" RETURNING Double),
    JSON_VALUE($double_json, "strict $.key" RETURNING Float);

-- From string to Utf8 and String
SELECT
    JSON_VALUE($string_json, "strict $.key" RETURNING Utf8),
    JSON_VALUE($string_json, "strict $.key" RETURNING String);

-- From timestamp to Datetime, Timestamp and Date
$date_json = CAST(@@{
    "date": 18312,
    "datetime": 1582223316,
    "timestamp": 1582223316529631
}@@ as Json);
SELECT
    JSON_VALUE($date_json, "strict $.date" RETURNING Date),
    JSON_VALUE($date_json, "strict $.datetime" RETURNING Datetime),
    JSON_VALUE($date_json, "strict $.timestamp" RETURNING Timestamp);

-- Null result form jsonpath
$_null_json = CAST(@@{
    "key": null
}@@ as Json);
SELECT
    JSON_VALUE($date_json, "strict $.key" RETURNING Int16);

-- Error case
-- Must return NULL because NULL ON ERROR is default
SELECT
    JSON_VALUE($int64_json, "strict $.key" RETURNING String);
