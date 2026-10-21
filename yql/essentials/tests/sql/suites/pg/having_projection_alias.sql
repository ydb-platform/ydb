--!syntax_pg
/* custom error: No such column: total */

SELECT sum(x) AS total FROM (SELECT 1 AS x) AS a HAVING total > 0;
