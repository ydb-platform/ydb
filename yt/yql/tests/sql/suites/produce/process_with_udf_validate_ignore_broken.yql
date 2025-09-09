/* postgres can not */

$processed = (
    process plato.Input0 using Person::New(key, subkey, Length(SimpleUdf::ReturnBrokenInt()))
);

PRAGMA config.flags("ValidateUdf", "None");
SELECT * FROM $processed;
