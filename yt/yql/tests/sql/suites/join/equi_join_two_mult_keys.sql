PRAGMA DisableSimpleColumns;
/* postgres can not */
SELECT * FROM plato.A JOIN plato.B ON A.key == B.key AND A.subkey == B.subkey;
