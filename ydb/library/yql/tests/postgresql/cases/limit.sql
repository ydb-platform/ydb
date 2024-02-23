-- Test null limit and offset.  The planner would discard a simple null
-- constant, so to ensure executor is exercised, do this:
select * from int8_tbl limit (case when random() < 0.5 then null::bigint end);
select * from int8_tbl offset (case when random() < 0.5 then null::bigint end);
-- Test assorted cases involving backwards fetch from a LIMIT plan node
begin;
rollback;
-- SKIP LOCKED and WITH TIES are incompatible
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 1 ROW WITH TIES FOR UPDATE SKIP LOCKED;
-- should fail
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		FETCH FIRST 2 ROW WITH TIES;
-- leave these views
