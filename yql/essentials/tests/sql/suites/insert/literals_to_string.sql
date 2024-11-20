/* postgres can not */
-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
insert into plato.Output (key, subkey, value) values (YQL::ToString(1), YQL::ToString(2), "3");
