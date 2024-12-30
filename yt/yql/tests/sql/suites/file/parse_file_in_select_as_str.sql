/* postgres can not */
select key, value, key in ParseFile('string', "keyid.lst") as privilege from plato.Input;
