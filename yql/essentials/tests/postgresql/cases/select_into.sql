-- Tests for WITH NO DATA and column name consistency
CREATE TABLE ctas_base (i int, j int);
INSERT INTO ctas_base VALUES (1, 2);
DROP TABLE ctas_base;
