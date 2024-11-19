/* syntax version 1 */
/* postgres can not */

-- In this case call to Json2::SqlValueNumber will be successfull, but cast
-- of -123 to Uint16 will fail
$json = CAST(@@{
    "key": -123
}@@ as Json);
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Uint16 ERROR ON ERROR);
