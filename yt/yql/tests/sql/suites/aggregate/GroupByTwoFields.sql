/* syntax version 1 */
select sum(c) as sum_c, max(d) as max_d from plato.Input group by a, b order by sum_c, max_d;
