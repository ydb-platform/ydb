/* multirun can not */
INSERT INTO plato.Output
SELECT
    *
FROM plato.Input
WHERE a IS NOT NULL;
COMMIT;

SELECT
    *
FROM plato.Output;
