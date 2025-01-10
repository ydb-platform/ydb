/* postgres can not */
$is_abc = Pire::Match(".*abc");
$_is_wtf = Pire::Match(".*wtf");

SELECT * FROM plato.Input WHERE $is_abc(value) LIMIT 10;
