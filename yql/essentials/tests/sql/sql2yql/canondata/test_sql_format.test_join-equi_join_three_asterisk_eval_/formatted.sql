PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    A.*,
    C.subkey AS goal,
    B.*,
    A.value || C.value AS ac_val_concat
FROM plato.A
JOIN plato.B
ON A.key == B.key
JOIN plato.C
ON B.subkey == C.subkey
ORDER BY
    A.key,
    goal,
    ac_val_concat;
