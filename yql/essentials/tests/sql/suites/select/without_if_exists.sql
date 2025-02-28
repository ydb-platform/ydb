
SELECT
    *
    without if exists x
FROM (select 1 as x,2 as z);


SELECT
    *
    without if exists y
FROM (select 1 as x, 2 as z);


