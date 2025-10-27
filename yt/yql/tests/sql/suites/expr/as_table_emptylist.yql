/* syntax version 1 */
/* postgres can not */
select * from as_table([]);
select x+1 from as_table([]);
select * from as_table([]) order by x limit 5 offset 2;
select x from as_table([]) order by x;
select count(*) from as_table([]);
select x, count(*) from as_table([]) group by x;
select x, count(*) from as_table([]) group by x having count(x) > 1;
select lead(x) over w, lag(x) over w, row_number() over w,
    count(*) over w from as_table([]) window w as ();

select lead(x) over w, lag(x) over w, rank() over w, denserank() over w,
    count(*) over w from as_table([]) window w as (order by x);

insert into plato.Output
select * from as_table([]);
