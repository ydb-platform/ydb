/* custom error:Cannot convert extracted JSON value to target type*/

-- In this case call to Json2::SqlValueNumber will fail because "string"
-- does not represent Number value
$json = CAST(@@{
    "key": "string"
}@@ as Json);
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Uint16 ERROR ON ERROR);
