select value from plato.Input group by value having avg(cast(key as int)) > 100 order by value;
