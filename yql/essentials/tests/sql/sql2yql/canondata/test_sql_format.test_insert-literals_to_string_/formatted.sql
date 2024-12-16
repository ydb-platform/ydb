/* postgres can not */
-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
INSERT INTO plato.Output (
    key,
    subkey,
    value
)
VALUES
    (YQL::ToString(1), YQL::ToString(2), '3')
;
