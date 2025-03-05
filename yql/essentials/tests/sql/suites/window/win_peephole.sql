/* syntax version 1 */
/* postgres can not */


$t = SELECT 'john' as name, 42 as age;

SELECT 
    SUM(age) OVER w0 AS sumAge, 
    LEAD(age,1) OVER w0 AS nextAge,
    LAG(age,1) OVER w0 AS prevAge,
    RANK() OVER w0 as rank,
    DENSE_RANK() OVER w0 as dense_rank,
    ROW_NUMBER() OVER w0 as row_number,
FROM $t AS u
WINDOW 
    w0 AS (ORDER BY name)
;

