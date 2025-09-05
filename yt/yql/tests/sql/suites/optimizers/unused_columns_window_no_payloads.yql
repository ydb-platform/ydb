USE plato;

select b from (
SELECT
    b,
    lag(a) over w as prev_a
FROM Input
WINDOW w AS (PARTITION BY b ORDER by c)
)
