/* postgres can not */
INSERT INTO plato.Output WITH truncate
SELECT
    a + b + c AS a,
    coalesce(d, "") AS b,
    f AS f,
    CAST(coalesce(e, TRUE) AS varchar) AS e
FROM
    plato.Input
;
