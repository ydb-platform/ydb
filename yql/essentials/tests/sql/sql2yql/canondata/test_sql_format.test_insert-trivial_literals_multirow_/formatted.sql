-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
/* postgres can not */
INSERT INTO plato.Output (
    key,
    subkey,
    value
)
VALUES
    ("1", "2", "3"),
    ("4", "5", "6"),
    ("7", "8", "9")
;
