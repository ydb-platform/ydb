/* postgres can not */
$processed = (
    PROCESS plato.Input0
    USING Person::New(key, subkey, Length(SimpleUdf::ReturnBrokenInt()))
);

PRAGMA config.flags("ValidateUdf", "None");

SELECT
    *
FROM
    $processed
;
