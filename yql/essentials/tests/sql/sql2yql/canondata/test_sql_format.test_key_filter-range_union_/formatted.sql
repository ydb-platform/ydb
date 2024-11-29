/* postgres can not */
SELECT
    *
FROM (
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key >= 9 --[8,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key >= 8 --[8,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key > 8 --[8,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key > 7 --[8,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key >= 7 --[7,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key <= 7 --(,9)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key <= 8 --(,9)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key < 8 --(,9)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key < 9 --(,9)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 8 OR key <= 9 --(,10)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key > 8 OR key < 8 --(,8),[9,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key > 8 OR key <= 8 --(,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key > 8 OR key < 9 --(,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key > 7 OR key < 9 --(,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key >= 7 OR key < 9 --(,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key > 7 OR key <= 9 --(,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key > 5 AND key < 7) OR (key > 4 AND key < 8) -- [5,8)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key > 5 AND key < 7) OR (key > 5 AND key < 8) -- [6,8)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key >= 5 AND key < 7) OR (key >= 5 AND key < 8) -- [5,8)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key > 5 AND key <= 7) OR (key > 4 AND key < 8) -- [5,8)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key > 5 AND key <= 7) OR (key > 4 AND key <= 7) -- [5,8)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key > 5 AND key < 8) OR (key > 4 AND key < 8) -- [5,8)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key == 1 AND subkey < 4 AND subkey > 1) OR (key == 2 AND subkey < 4 AND subkey > 1) --[{1,2},{1,4}),[{2,2},{2,4})
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key == 1 AND subkey < 3 AND subkey > 1) OR (key == 2 AND subkey < 3 AND subkey > 1) --[{1,2}],[{2,2}]
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key == 1 AND subkey > 1) OR (key == 2 AND subkey < 3) --[{1,2},{1}],[{2},{2,3})
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key == 1 AND subkey < 3 AND subkey > 1) OR key == 1 --[1]
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key == 1 AND subkey > 1) OR key == 1 --[1]
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE (key == 1 AND subkey < 4 AND subkey > 1) OR (key > 1) --[{1,2},{1,4}),[2,)
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 1 AND (subkey BETWEEN 2 AND 3 OR subkey < 2) --[{1},{1,3}]
    UNION ALL
    SELECT
        *
    FROM plato.Input
    WHERE key == 1 AND (subkey >= 2 OR subkey < 2) --[1]
)
ORDER BY
    key,
    subkey;
