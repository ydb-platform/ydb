SELECT COUNT(DISTINCT (s.S_I_ID)) AS STOCK_COUNT
FROM `/Root/test/tpcc/order_line` as ol INNER JOIN `/Root/test/tpcc/stock` as s ON s.S_I_ID = ol.OL_I_ID
WHERE ol.OL_W_ID = 1
AND ol.OL_D_ID = 10
AND ol.OL_O_ID < 3000
AND ol.OL_O_ID >= 2900
AND s.S_W_ID = 1
AND s.S_QUANTITY < 15
