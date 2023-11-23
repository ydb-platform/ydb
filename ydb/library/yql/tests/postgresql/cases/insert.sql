create table inserttest (f1 int, f2 int[],
                         f3 insert_test_type, f4 insert_test_type[]);
-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text);
drop table inserttest2;
drop table inserttest;
create table mlparted2 (b int not null, a int not null);
create table mlparted5a (a int not null, c text, b int not null);
create table mlparted5_b (d int, b int, c text, a int);
create table donothingbrtrig_test1 (b text, a int);
create table donothingbrtrig_test2 (c text, b text, a int);
create table returningwrtest2 (b text, c int, a int);
