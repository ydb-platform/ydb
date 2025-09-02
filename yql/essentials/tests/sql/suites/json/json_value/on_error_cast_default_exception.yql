/* custom error:Failed to cast default value from ON ERROR clause to target type Optional<Uint16>*/

-- Here JsonPath engine returns error result and ON ERROR section must be used.
-- But default value in ON ERROR section is -123 and casting it to Uint16 will fail.
-- In this case exception must be raised.
$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "strict $.key" RETURNING Uint16 DEFAULT -123 ON ERROR);
