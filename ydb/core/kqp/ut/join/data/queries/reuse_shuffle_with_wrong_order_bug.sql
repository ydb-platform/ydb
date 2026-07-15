PRAGMA TablePathPrefix = "/Root/test/tpcc/";

$semi_join = (
    SELECT *
    FROM customer c
    LEFT SEMI JOIN oorder o
      ON c.C_W_ID = o.O_W_ID
     AND c.C_D_ID = o.O_D_ID
);

SELECT s.C_W_ID, s.C_D_ID, n.NO_O_ID
FROM $semi_join s
INNER JOIN new_order as n
  ON s.C_W_ID = n.NO_W_ID
 AND s.C_D_ID = n.NO_D_ID;
