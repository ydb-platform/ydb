/* syntax version 1 */
/* postgres can not */

$json = CAST(@@{
    "key": 123
}@@ as Json);

-- Tuple type is not supported for variables
SELECT
    JSON_VALUE(
        $json, "strict $var"
        PASSING
            AsTuple(1, 2) as var
    );