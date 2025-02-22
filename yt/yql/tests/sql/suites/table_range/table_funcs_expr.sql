/* postgres can not */
/* syntax version 1 */
use plato;
select count(*) from concat("Inp"||Unicode::ToLower("ut"u));
select count(*) from concat_strict("Inp"||Unicode::ToLower("ut"u));
select count(*) from range("","Inp" || Unicode::ToLower("ut"u));
select count(*) from range_strict("","Inp" || Unicode::ToLower("ut"u));
select count(*) from filter("",($x)->{return $x == "Input"});
select count(*) from filter_strict("",($x)->{return $x == "Input"});
select count(*) from like("","Inp" || "%");
select count(*) from like_strict("","Inp" || "%");
select count(*) from regexp("","Inp" || ".t");
select count(*) from regexp_strict("","Inp" || ".t");
select count(*) from each(AsList("Input"));
select count(*) from each_strict(AsList("Input"));
select count(*) from folder(SUBSTRING("foo",0,0));
