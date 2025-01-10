(SELECT * FROM plato.Input WHERE key < "100" LIMIT 2)
UNION ALL
(SELECT * FROM plato.Input WHERE key > "100" LIMIT 2);
