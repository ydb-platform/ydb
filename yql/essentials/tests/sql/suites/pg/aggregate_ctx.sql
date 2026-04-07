--!syntax_pg
select string_agg(x,',') from (values ('a'),('b')) a(x);
