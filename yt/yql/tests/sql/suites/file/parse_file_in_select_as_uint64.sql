/* postgres can not */
select key, value, cast(key as int32) in ParseFile('uint64', "keyid.lst") as privilege from plato.Input;
