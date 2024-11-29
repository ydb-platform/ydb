/* postgres can not */
$processed = (
    PROCESS plato.Input0
    USING Person::New(key, subkey, coalesce(CAST(value AS Uint32), 0))
);
PRAGMA config.flags("ValidateUdf", "Lazy");

SELECT
    *
FROM $processed;
