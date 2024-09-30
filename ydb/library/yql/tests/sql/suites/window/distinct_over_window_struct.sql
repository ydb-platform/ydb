/* syntax version 1 */
/* postgres can not */

USE plato;

PRAGMA DistinctOverWindow;

$input = AsList( 
    AsStruct(1 AS key, 1 AS subkey, AsStruct(1 AS i1, 2 AS i2, 3 AS i3) AS col), 
    AsStruct(2 AS key, 1 AS subkey, AsStruct(1 AS i1, 2 AS i2, 3 AS i3) AS col),
    AsStruct(3 AS key, 1 AS subkey, AsStruct(1 AS i1, 2 AS i2, 3 AS i3) AS col),
    AsStruct(4 AS key, 2 AS subkey, AsStruct(3 AS i1, 4 AS i2, 5 AS i3) AS col),
    AsStruct(5 AS key, 2 AS subkey, AsStruct(3 AS i1, 4 AS i2, 5 AS i3) AS col),
    AsStruct(6 AS key, 2 AS subkey, AsStruct(5 AS i1, 5 AS i2, 5 AS i3) AS col),
    AsStruct(7 AS key, 3 AS subkey, AsStruct(5 AS i1, 6 AS i2, 7 AS i3) AS col),
    AsStruct(8 AS key, 3 AS subkey, AsStruct(6 AS i1, 7 AS i2, 8 AS i3) AS col),
    AsStruct(9 AS key, 3 AS subkey, AsStruct(7 AS i1, 8 AS i2, 9 AS i3) AS col),
);

SELECT
    key,
    subkey,
    col,
    -- assuming ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    count(DISTINCT col) OVER (PARTITION BY subkey ORDER BY key ASC) AS cnt1_asc,
    count(DISTINCT col) OVER (PARTITION BY subkey ORDER BY key DESC) AS cnt2_desc,
FROM AS_TABLE($input)
ORDER BY key;

SELECT
    key,
    subkey,
    col,
    -- assuming ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    count(DISTINCT col) OVER (PARTITION BY subkey) AS cnt,
FROM AS_TABLE($input)
ORDER BY key;
