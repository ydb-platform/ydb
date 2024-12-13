USE plato;

INSERT INTO Output
SELECT
    (waz, wdz, wtz),
    waz,
    wdz,
    wtz
FROM
    Input
;
