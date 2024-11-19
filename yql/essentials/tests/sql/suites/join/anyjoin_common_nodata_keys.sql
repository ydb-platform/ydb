/* postgres can not */
/* syntax version 1 */

USE plato;

$a = SELECT AsTuple(1, 2) AS k1, 1 AS k2, 2 AS v;

$b = SELECT AsTuple(1, 2) AS k1, 1 AS k2, 3 AS v1
     UNION ALL
     SELECT AsTuple(1, 2) AS k1, 1 AS k2, 3 AS v1;

INSERT INTO @a
SELECT * FROM $a;

INSERT INTO @b
SELECT * FROM $b;

COMMIT;

SELECT * FROM @a AS a LEFT JOIN ANY @b AS b using(k1,k2);
SELECT * FROM @a AS a LEFT JOIN     @b AS b using(k1,k2);


