/* postgres can not */
insert into plato.Output with truncate
select a + b + c as a, coalesce(d, "") as b, f as f, cast(coalesce(e, true) as varchar) as e from plato.Input;

