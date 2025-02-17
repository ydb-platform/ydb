/* syntax version 1 */
select value, avg(cast(key as int)) + 0.3 as key from plato.Input group by value having value > "foo" order by key;