-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
insert into plato.Output (key, subkey, value) select key, subkey, max(value) from plato.Input group by key, subkey order by key;
