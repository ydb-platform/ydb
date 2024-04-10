--
-- CREATE_TABLE
--
--
-- CLASS DEFINITIONS
--
CREATE TABLE hobbies_r (
	name		text,
	person 		text
);
CREATE TABLE equipment_r (
	name 		text,
	hobby		text
);
CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
CREATE TABLE person (
	name 		text,
	age			int4,
	location 	point
);
CREATE TABLE city (
	name		name,
	location 	box,
	budget 		city_budget
);
CREATE TABLE dept (
	dname		name,
	mgrname 	text
);
CREATE TABLE slow_emp4000 (
	home_base	 box
);
CREATE TABLE fast_emp4000 (
	home_base	 box
);
CREATE TABLE road (
	name		text,
	thepath 	path
);
CREATE TABLE real_city (
	pop			int4,
	cname		text,
	outline 	path
);
--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--
CREATE TABLE a_star (
	class		char,
	a 			int4
);
CREATE TABLE aggtest (
	a 			int2,
	b			float4
);
CREATE TABLE hash_i4_heap (
	seqno 		int4,
	random 		int4
);
CREATE TABLE hash_name_heap (
	seqno 		int4,
	random 		name
);
CREATE TABLE hash_txt_heap (
	seqno 		int4,
	random 		text
);
CREATE TABLE hash_f8_heap (
	seqno		int4,
	random 		float8
);
-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
--
-- CREATE TABLE hash_ovfl_heap (
--	x			int4,
--	y			int4
-- );
CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
);
CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
);
CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);
CREATE TABLE bt_f8_heap (
	seqno 		float8,
	random 		int4
);
CREATE TABLE array_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);
CREATE TABLE array_index_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);
CREATE TABLE testjsonb (
       j jsonb
);
CREATE TABLE IF NOT EXISTS test_tsvector(
	t text,
	a tsvector
);
CREATE TEMP TABLE explicitly_temp (a int primary key);			-- also OK
-- check that tables with oids cannot be created anymore
CREATE TABLE withoid() WITH OIDS;
-- but explicitly not adding oids is still supported
CREATE TEMP TABLE withoutoid() WITHOUT OIDS;
DROP TABLE withoutoid;
-- Verify that subtransaction rollback restores rd_createSubid.
BEGIN;
CREATE TABLE remember_create_subid (c int);
SAVEPOINT q;
DROP TABLE remember_create_subid;
ROLLBACK TO q;
COMMIT;
-- Verify that subtransaction rollback restores rd_firstRelfilenodeSubid.
CREATE TABLE remember_node_subid (c int);
BEGIN;
SAVEPOINT q;
DROP TABLE remember_node_subid;
ROLLBACK TO q;
COMMIT;
-- syntax does not allow empty list of values for list partitions
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN ();
-- check if compatible with the specified parent
-- cannot create as partition of a non-partitioned table
CREATE TABLE unparted (
	a int
);
DROP TABLE unparted;
SELECT conislocal, coninhcount FROM pg_constraint WHERE conrelid = 'part_b'::regclass;
create table defcheck_def (a int, c int, b int);
-- test that complex default partition constraints are enforced correctly
insert into defcheck_def values (0, 0);
