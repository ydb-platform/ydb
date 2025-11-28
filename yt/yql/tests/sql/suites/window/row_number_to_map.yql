/* postgres can not */
/* syntax version 1 */

USE plato;

SELECT key, subkey,
       ROW_NUMBER() OVER w1 AS rn1,
       ROW_NUMBER() OVER w2 AS rn2,
       COUNT(*)     OVER w2 AS w2_cnt,
       ROW_NUMBER() OVER w3 AS rn3,
       ROW_NUMBER() OVER w4 AS rn4,
FROM Input
WINDOW
       w1 AS (),
       w2 AS (ORDER BY subkey      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
       w3 AS (ORDER BY subkey DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
       w4 AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY subkey;
