/* custom error: Expected numeric type, but got String */

USE plato;

select * from (
SELECT
    a.key as x, sum(b.value)
FROM Input as a
JOIN Input as b
USING (key)
GROUP BY a.key
) where x > "aaa"
ORDER BY x;

select 1;
select 1;
select 1;
select 1;
select 1;
select 1;
