/* postgres can not */
/* syntax version 1 */

USE plato;

SELECT
       ROW_NUMBER() OVER w AS rn,
       COUNT(*)     OVER w AS cnt,
FROM Input
WINDOW
       w AS ()
ORDER BY rn;
