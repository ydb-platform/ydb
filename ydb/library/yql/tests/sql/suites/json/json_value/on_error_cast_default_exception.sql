/* syntax version 1 */
/* postgres can not */

-- Here JsonPath engine returns error result and ON ERROR section must be used.
-- But default value in ON ERROR section is -123 and casting it to Uint16 will fail.
-- In this case exception must be raised.
$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Uint16 DEFAULT -123 ON ERROR);
