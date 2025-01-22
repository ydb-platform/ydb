/* postgres can not */
USE plato;

$list = (select item FROM (select AsList('foo', 'bar', 'baz', 'quux', 'bat') as `list`) FLATTEN BY `list` as item);

--INSERT INTO Output
SELECT
  item, YQL::Concat('+', Lead(item, 1) over w), YQL::Concat("++", Lead(item,2) over w)
FROM $list
WINDOW w as ();
