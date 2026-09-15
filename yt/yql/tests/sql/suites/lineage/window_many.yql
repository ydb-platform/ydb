insert into plato.Output
select key, row_number() over w1, lag(value) over w1, lead(value) over w1,
    rank(value) over w2, dense_rank(value) over w2
    from plato.Input window w1 as (order by key), w2 as (order by key desc)