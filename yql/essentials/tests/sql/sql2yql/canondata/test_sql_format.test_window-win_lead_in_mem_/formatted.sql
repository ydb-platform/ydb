/* postgres can not */
USE plato;

$list = (
    SELECT
        item
    FROM (
        SELECT
            AsList('foo', 'bar', 'baz', 'quux', 'bat') AS `list`
    )
        FLATTEN BY `list` AS item
);

--INSERT INTO Output
SELECT
    item,
    YQL::Concat('+', Lead(item, 1) OVER w),
    YQL::Concat("++", Lead(item, 2) OVER w)
FROM $list
WINDOW
    w AS ();
