PRAGMA DisableSimpleColumns;
/* postgres can not */
SELECT
    count(i1.key) as count,
    count(distinct i1.key) as uniq_count
FROM plato.Input as i1 JOIN plato.Input AS i2 on cast(i1.key as uint32) / 100 == cast(i2.subkey as uint32) / 100
;
