/* postgres can not */
INSERT INTO plato.Output WITH truncate
SELECT
    value
FROM plato.Input;
