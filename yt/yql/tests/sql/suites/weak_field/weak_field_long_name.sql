/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  WeakField(long_long_long_column_name, 'string')
FROM Input
