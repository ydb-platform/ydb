/* custom error:Expected data or optional of data, but got: Tuple<Int32,Int32>*/

$json = CAST(@@{
    "key": 123
}@@ as Json);

-- Tuple type is not supported for variables
SELECT
    JSON_EXISTS(
        $json, "strict $var"
        PASSING
            AsTuple(1, 2) as var
    );
