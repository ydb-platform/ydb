/* postgres can not */
PROCESS plato.Input0
USING Person::New(key, subkey, coalesce(CAST(value AS Uint32), 0));
