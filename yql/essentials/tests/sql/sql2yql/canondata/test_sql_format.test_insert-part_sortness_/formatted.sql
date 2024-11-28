/* postgres can not */
INSERT INTO plato.Output WITH truncate
SELECT
    key,
    value
FROM plato.Input;
