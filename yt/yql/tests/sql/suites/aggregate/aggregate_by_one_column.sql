/* syntax version 1 */
select key, "WAT" as subkey, Max(value) as value from plato.Input group by key order by key;