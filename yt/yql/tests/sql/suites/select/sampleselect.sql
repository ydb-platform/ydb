/* postgres can not */
pragma sampleselect;
select key, subkey, value || "foo" as new_value from plato.Input;
