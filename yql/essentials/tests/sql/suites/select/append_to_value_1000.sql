/* postgres can not */
/* kikimr can not - no ref select mode */
select key, subkey, value || "foo" as new_value from plato.Input;
