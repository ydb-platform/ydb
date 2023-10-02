/* postgres can not */
use plato;
pragma SimpleColumns;

SELECT *
FROM `Input2` AS a
LEFT SEMI JOIN `Input3` AS b
ON a.value == b.value;

SELECT *
FROM `Input2` AS a
LEFT ONLY JOIN `Input3` AS b
ON a.value == b.value;

SELECT *
FROM `Input2` AS a
RIGHT SEMI JOIN `Input3` AS b
ON a.value == b.value;

SELECT *
FROM `Input2` AS a
RIGHT ONLY JOIN `Input3` AS b
ON a.value == b.value;
