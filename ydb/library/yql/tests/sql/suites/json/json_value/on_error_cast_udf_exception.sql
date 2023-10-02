/* syntax version 1 */
/* postgres can not */

-- In this case call to Json2::SqlValueNumber will fail because "string"
-- does not represent Number value
$json = CAST(@@{
    "key": "string"
}@@ as Json);
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Uint16 ERROR ON ERROR);
