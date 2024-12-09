INSERT INTO plato.Output
SELECT
    key || value
FROM plato.Input;
