/* syntax version 1 */
/* postgres can not */
SELECT
   String::HexText(String::HexText(value)) as value
FROM plato.Input4
GROUP BY value
ORDER BY value;
