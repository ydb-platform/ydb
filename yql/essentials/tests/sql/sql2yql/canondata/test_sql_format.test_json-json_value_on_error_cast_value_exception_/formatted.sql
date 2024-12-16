/* custom error:Failed to cast extracted JSON value to target type Optional<Uint16>*/
-- In this case call to Json2::SqlValueNumber will be successfull, but cast
-- of -123 to Uint16 will fail
$json = CAST(
    @@{
    "key": -123
}@@ AS Json
);

SELECT
    JSON_VALUE ($json, 'strict $.key' RETURNING Uint16 ERROR ON ERROR)
;
