--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');
--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));
drop table inserttest;
create table inserttest (f1 int, f2 int[],
                         f3 insert_test_type, f4 insert_test_type[]);
-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text);
drop table inserttest2;
drop table inserttest;
create table mlparted2 (b int not null, a int not null);
create table mlparted5a (a int not null, c text, b int not null);
create table mlparted5_b (d int, b int, c text, a int);
-- check that the message shows the appropriate column description in a
-- situation where the partitioned table is not the primary ModifyTable node
create table inserttest3 (f1 text default 'foo', f2 text default 'bar', f3 int);
drop table inserttest3;
create table donothingbrtrig_test1 (b text, a int);
create table donothingbrtrig_test2 (c text, b text, a int);
create table returningwrtest2 (b text, c int, a int);
