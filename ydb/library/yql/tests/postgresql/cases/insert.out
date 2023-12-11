--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');
--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));
