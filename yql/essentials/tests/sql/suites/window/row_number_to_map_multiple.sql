/* postgres can not */
/* syntax version 1 */

USE plato;

SELECT key, subkey,
       ROW_NUMBER() OVER w1 AS rn1,
       ROW_NUMBER() OVER w2 AS rn2,
       ROW_NUMBER() OVER w3 AS rn3,
FROM Input
WINDOW
       w1 AS (),
       w2 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
       w3 AS (ORDER BY subkey DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY subkey;
