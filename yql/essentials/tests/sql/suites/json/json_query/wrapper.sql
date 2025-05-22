/* syntax version 1 */
/* postgres can not */

$json = CAST(@@{
    "object": {"key": 123},
    "multi_object": [{"key": 123}, {"key": 456}, {"key": 789}],
    "array": [1, 2, 3],
    "multi_array": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
}@@ as Json);

-- Check JSON_QUERY without wrapper
SELECT
    JSON_QUERY($json, "strict $.array"), -- defaults to WITHOUT ARRAY WRAPPER
    JSON_QUERY($json, "strict $.array" WITHOUT WRAPPER),
    JSON_QUERY($json, "strict $.array" WITHOUT ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.object"), -- defaults to WITHOUT ARRAY WRAPPER
    JSON_QUERY($json, "strict $.object" WITHOUT WRAPPER),
    JSON_QUERY($json, "strict $.object" WITHOUT ARRAY WRAPPER);

-- Check UNCONDITIONAL wrapper
SELECT
    JSON_QUERY($json, "strict $.array" WITH WRAPPER), -- default to WITH UNCONDITIONAL WRAPPER
    JSON_QUERY($json, "strict $.array" WITH ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.array" WITH UNCONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.array" WITH UNCONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.object" WITH WRAPPER), -- default to WITH UNCONDITIONAL WRAPPER
    JSON_QUERY($json, "strict $.object" WITH ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.object" WITH UNCONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.object" WITH UNCONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.array[*]" WITH WRAPPER), -- default to WITH UNCONDITIONAL WRAPPER
    JSON_QUERY($json, "strict $.array[*]" WITH ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.array[*]" WITH UNCONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.array[*]" WITH UNCONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.multi_array[*]" WITH WRAPPER), -- default to WITH UNCONDITIONAL WRAPPER
    JSON_QUERY($json, "strict $.multi_array[*]" WITH ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.multi_array[*]" WITH UNCONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.multi_array[*]" WITH UNCONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.multi_object[*]" WITH WRAPPER), -- default to WITH UNCONDITIONAL WRAPPER
    JSON_QUERY($json, "strict $.multi_object[*]" WITH ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.multi_object[*]" WITH UNCONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.multi_object[*]" WITH UNCONDITIONAL ARRAY WRAPPER);

-- Check CONDITIONAL wrapper
SELECT
    JSON_QUERY($json, "strict $.array" WITH CONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.array" WITH CONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.object" WITH CONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.object" WITH CONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.array[*]" WITH CONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.array[*]" WITH CONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.multi_array[*]" WITH CONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.multi_array[*]" WITH CONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($json, "strict $.multi_object[*]" WITH CONDITIONAL WRAPPER),
    JSON_QUERY($json, "strict $.multi_object[*]" WITH CONDITIONAL ARRAY WRAPPER);
