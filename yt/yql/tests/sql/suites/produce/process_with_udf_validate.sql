/* postgres can not */
$processed = (
    process plato.Input0 using Person::New(key, subkey, coalesce(cast(value as Uint32), 0))
);

PRAGMA config.flags("ValidateUdf", "Lazy");
SELECT * FROM $processed;
