/* syntax version 1 */
/* postgres can not */

-- Accessing absent object member will cause jsonpath error in strict mode
$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "strict $.key"), -- defaults to NULL ON ERROR
    JSON_VALUE($json, "strict $.key" NULL ON ERROR),
    JSON_VALUE($json, "strict $.key" DEFAULT "*** error ***" ON ERROR);

-- Null as a default value
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Uint16 DEFAULT NULL ON ERROR);

-- Check that default value is casted to the target type
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Int16 DEFAULT "123" ON ERROR),
    JSON_VALUE($json, "strict $.key" RETURNING Int16 DEFAULT 123.456 ON ERROR);

-- Here values retrieved from JSON cannot be casted to the target type Int16.
-- ON ERROR default value must be used instead
$invalid_types_json = CAST(@@{
    "key": "string",
    "another_key": -123
}@@ as Json);
SELECT
    JSON_VALUE($invalid_types_json, "strict $.key" RETURNING Int16 DEFAULT 456 ON ERROR),
    JSON_VALUE($invalid_types_json, "strict $.another_key" RETURNING Uint16 DEFAULT 456 ON ERROR);