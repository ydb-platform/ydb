/* postgres can not */
/* multirun can not */
-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
insert into plato.Output with truncate (key, subkey, value) select key, subkey, value from plato.Input;
commit;
select coalesce(key, ""), coalesce(subkey, ""), coalesce(value, "") from plato.Output;
