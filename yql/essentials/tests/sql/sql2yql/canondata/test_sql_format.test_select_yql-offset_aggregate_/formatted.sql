/* custom error: Aggregate functions are not allowed in this context */
PRAGMA YqlSelect = 'force';

SELECT
    1
LIMIT 1 OFFSET sum(1);
