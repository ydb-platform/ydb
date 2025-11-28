/* postgres can not */
/* multirun can not */
-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
insert into plato.Output (key, subkey, new_value, one_more_value) select key, subkey, value as new_value, "x" from plato.Input;
commit;
select key, subkey, new_value, one_more_value from plato.Output;
