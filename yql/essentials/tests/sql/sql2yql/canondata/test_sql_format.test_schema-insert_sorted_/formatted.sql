/* multirun can not */
INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    d,
    a,
    b,
    c
;
COMMIT;

SELECT
    *
FROM
    plato.Output
;
