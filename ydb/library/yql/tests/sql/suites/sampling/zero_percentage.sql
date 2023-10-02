/* postgres can not */
SELECT * FROM plato.Input TABLESAMPLE BERNOULLI(0) ORDER BY subkey;
