PRAGMA DisableSimpleColumns;
/* postgres can not */
SELECT A.*, C.subkey as goal,B.*
FROM plato.A
JOIN plato.B ON A.key == B.key
JOIN plato.C ON B.subkey == C.subkey
ORDER BY A.key;
