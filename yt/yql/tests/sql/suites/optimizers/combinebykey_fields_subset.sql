SELECT count(DISTINCT subkey) AS subkey
FROM plato.Input
WHERE (key == "075" OR key == "150");
