-- bug #5049: early 8.4.x chokes on volatile DISTINCT ON clauses
select distinct on (1) floor(random()) as r, f1 from int4_tbl order by 1,2;
