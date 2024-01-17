-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);
-- corner case: VALUES with no columns
CREATE TEMP TABLE nocols();
--
-- Test ORDER BY options
--
CREATE TEMP TABLE foo (f1 int);
INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);
-- check if indexscans do the right things
CREATE INDEX fooi ON foo (f1);
CREATE INDEX fooi ON foo (f1 DESC);
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);
-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
select * from (values (2),(null),(1)) v(k) where k = k order by k;
select * from (values (2),(null),(1)) v(k) where k = k;
