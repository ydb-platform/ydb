/* syntax version 1 */
/* postgres can not */

-- Accessing absent object member will cause empty result in lax mode
$json = CAST("{}" as Json);
SELECT
    JSON_VALUE($json, "lax $.key"), -- defaults to NULL ON EMPTY
    JSON_VALUE($json, "lax $.key" NULL ON EMPTY),
    JSON_VALUE($json, "lax $.key" DEFAULT "*** empty ***" ON EMPTY);

-- Null as a default value
SELECT
    JSON_VALUE($json, "lax $.key" RETURNING Uint16 DEFAULT NULL ON EMPTY);

-- Check that default value is casted to the target type
SELECT
    JSON_VALUE($json, "lax $.key" RETURNING Int16 DEFAULT "123" ON EMPTY),
    JSON_VALUE($json, "lax $.key" RETURNING Int16 DEFAULT 123.456 ON EMPTY);

-- Here JsonPath engine returns empty result and ON EMPTY section must be used.
-- But default value in ON EMPTY section is -123 and casting it to Uint16 will fail.
-- In this case ON ERROR section must be returned.
SELECT
    JSON_VALUE(
        $json,
        "lax $.key"
        RETURNING Uint16
        DEFAULT -123 ON EMPTY
        DEFAULT 456 ON ERROR
    );