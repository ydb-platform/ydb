PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    count(i1.key) AS count,
    count(DISTINCT i1.key) AS uniq_count
FROM
    plato.Input AS i1
JOIN
    plato.Input AS i2
ON
    CAST(i1.key AS uint32) / 100 == CAST(i2.subkey AS uint32) / 100
;
