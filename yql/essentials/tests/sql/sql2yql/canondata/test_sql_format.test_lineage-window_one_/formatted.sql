INSERT INTO plato.Output
SELECT
    key,
    row_number() OVER ()
FROM plato.Input;
