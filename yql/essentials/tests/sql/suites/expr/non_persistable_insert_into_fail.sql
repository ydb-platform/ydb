PRAGMA Warning("error", '1104');

USE plato;

INSERT INTO
    Output (key, value)
VALUES
    ("foo", YQL::NewMTRand(1));
