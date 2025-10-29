/* postgres can not */
USE plato;

$i = (SELECT * FROM Input2);
$ii = (SELECT * FROM Input4);

$count_a=(SELECT count(*) FROM $i WHERE substring(value, 0, 1) = 'a');

--INSERT INTO Output
SELECT
  count(*), count(*)/$count_a
FROM $i;

--INSERT INTO Output
SELECT
  count(*), count(*)/$count_a
FROM $ii;
