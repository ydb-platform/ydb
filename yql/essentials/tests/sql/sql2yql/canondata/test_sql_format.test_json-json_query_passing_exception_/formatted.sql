/* syntax version 1 */
/* postgres can not */
/* custom error:Expected data or optional of data, but got: Tuple<Int32,Int32>*/
$json = CAST(
    @@{
    "key": 123
}@@ AS Json
);

-- Tuple type is not supported for variables
SELECT
    JSON_QUERY (
        $json, "strict $var" PASSING AsTuple(1, 2) AS var WITH UNCONDITIONAL ARRAY WRAPPER
    )
;
