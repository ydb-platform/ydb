pragma UseBlocks;
USE plato;

SELECT
    a || a, a || 'x'p, c + c, c * 10p, Pg::upper(a)
FROM Input
