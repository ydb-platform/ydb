/* postgres can not */

SELECT count(DISTINCT subkey) AS subkey
FROM plato.CONCAT(Input0, Input1)
WHERE (key == "075" OR key == "150");
