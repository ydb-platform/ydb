/* postgres can not */
select key, subkey, SimpleUdf::Concat(value, "test") as value from plato.Input;