--!syntax_v1
$in = (SELECT * FROM InputJoin1 WHERE Value = "Value1");
SELECT *
FROM $in AS t1
     RIGHT SEMI JOIN InputJoin2 AS t2
       ON t1.Fk21 = t2.Key1
ORDER BY Key1, Key2;

SELECT *
FROM InputJoin4 AS t1
     RIGHT SEMI JOIN InputJoin2 AS t2
       ON t1.Key1 = t2.Key1 AND t1.Key2 = t2.Key2
ORDER BY Key1, Key2;

SELECT *
FROM InputJoin2 AS t1
     RIGHT SEMI JOIN InputJoinIndex2 VIEW Index AS t2
       ON t1.Key1 = t2.Fk2
ORDER BY Key1, Key2;
