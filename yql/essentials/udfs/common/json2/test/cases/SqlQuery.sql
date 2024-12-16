/* syntax version 1 */

$path = Json2::CompilePath("strict $.x");
$array = CAST("[]" as Json);
$object = CAST("{}" as Json);

-- Valid cases
$nested_object = CAST(@@{"x": {"y": 123}}@@ as Json);
$nested_array = CAST(@@{"x": [123, 456]}@@ as Json);
SELECT
    Json2::SqlQuery($nested_object, $path, AsDict(), false, $array, false, $object),
    Json2::SqlQuery($nested_array, $path, AsDict(), false, $array, false, $object);

-- Null handling
SELECT
    Json2::SqlQuery(NULL, $path, AsDict(), false, $array, false, $object);

-- Errors
$jsonpath_error = CAST(@@{"y": []}@@ as Json);
SELECT
    Json2::SqlQuery($jsonpath_error, $path, AsDict(), false, $array, false, NULL),
    Json2::SqlQuery($jsonpath_error, $path, AsDict(), false, $array, false, $object),
    Json2::SqlQuery($jsonpath_error, $path, AsDict(), false, $object, false, $array);

$mismatch_error = CAST(@@{"x": 123}@@ as Json);
SELECT
    Json2::SqlQuery($mismatch_error, $path, AsDict(), false, $array, false, NULL),
    Json2::SqlQuery($mismatch_error, $path, AsDict(), false, $array, false, $object),
    Json2::SqlQuery($mismatch_error, $path, AsDict(), false, $object, false, $array);

-- Wrap
$nested_value = CAST(@@{"x": 123}@@ as Json);
SELECT
    Json2::SqlQueryWrap($nested_value, $path, AsDict(), false, $object, false, $array),
    Json2::SqlQueryConditionalWrap($nested_value, $path, AsDict(), false, $object, false, $array);

SELECT
    Json2::SqlQuery($nested_object, $path, AsDict(), false, $object, false, $array),
    Json2::SqlQueryWrap($nested_object, $path, AsDict(), false, $object, false, $array),
    Json2::SqlQueryConditionalWrap($nested_object, $path, AsDict(), false, $object, false, $array);

SELECT
    Json2::SqlQuery($nested_array, $path, AsDict(), false, $object, false, $array),
    Json2::SqlQueryWrap($nested_array, $path, AsDict(), false, $object, false, $array),
    Json2::SqlQueryConditionalWrap($nested_array, $path, AsDict(), false, $object, false, $array);

-- Wrap empty result
$path_lax = Json2::CompilePath("lax $.x");
SELECT
    Json2::SqlQuery($object, $path_lax, AsDict(), false, $object, false, $object),
    Json2::SqlQueryWrap($object, $path_lax, AsDict(), false, $object, false, $object),
    Json2::SqlQueryConditionalWrap($object, $path_lax, AsDict(), false, $object, false, $object);