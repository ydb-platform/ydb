/* postgres can not */
/* kikimr can not - range not supported */
select * from plato.concat("Input1", "Input2") order by key, subkey;
