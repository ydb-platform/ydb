-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);
-- corner case: VALUES with no columns
CREATE TEMP TABLE nocols();
--
-- Test ORDER BY options
--
CREATE TEMP TABLE foo (f1 int);
INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);
