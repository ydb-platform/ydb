/* postgres can not */
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM plato.Input
LIMIT 2;
