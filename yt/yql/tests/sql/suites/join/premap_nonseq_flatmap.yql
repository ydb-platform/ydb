PRAGMA DisableSimpleColumns;
use plato;

$hashes = (
  SELECT multiplier AS hash
  FROM (SELECT ListFromRange(0, 3) AS multiplier)
  FLATTEN BY multiplier
);

SELECT * FROM Input1 AS a CROSS JOIN $hashes AS h
ORDER BY a.key,a.subkey,h.hash;
