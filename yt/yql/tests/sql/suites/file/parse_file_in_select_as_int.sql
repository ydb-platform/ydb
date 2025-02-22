/* postgres can not */
select key, value, cast(key as int32) ?? 0 in ParseFile('int32', "keyid.lst") as privilege from plato.Input;
