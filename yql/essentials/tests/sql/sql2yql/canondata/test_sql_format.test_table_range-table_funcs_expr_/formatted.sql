/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    count(*)
FROM
    concat('Inp' || Unicode::ToLower("ut"u))
;

SELECT
    count(*)
FROM
    concat_strict('Inp' || Unicode::ToLower("ut"u))
;

SELECT
    count(*)
FROM
    range('', 'Inp' || Unicode::ToLower("ut"u))
;

SELECT
    count(*)
FROM
    range_strict('', 'Inp' || Unicode::ToLower("ut"u))
;

SELECT
    count(*)
FROM
    filter('', ($x) -> {
        RETURN $x == 'Input';
    })
;

SELECT
    count(*)
FROM
    filter_strict('', ($x) -> {
        RETURN $x == 'Input';
    })
;

SELECT
    count(*)
FROM
    like('', 'Inp' || '%')
;

SELECT
    count(*)
FROM
    like_strict('', 'Inp' || '%')
;

SELECT
    count(*)
FROM
    regexp('', 'Inp' || '.t')
;

SELECT
    count(*)
FROM
    regexp_strict('', 'Inp' || '.t')
;

SELECT
    count(*)
FROM
    each(AsList('Input'))
;

SELECT
    count(*)
FROM
    each_strict(AsList('Input'))
;

SELECT
    count(*)
FROM
    folder(SUBSTRING('foo', 0, 0))
;
