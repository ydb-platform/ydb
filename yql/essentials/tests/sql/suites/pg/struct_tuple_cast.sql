/* postgres can not */
/* dq can not */
/* dqfile can not */

$s = <|foo:1p|>;
$t = (1p,);

select cast($s as Struct<a:PgText,b:PgInt4,c:_PgText,foo:PgInt4>);
select cast($t as Tuple<PgInt4,PgText,_PgText>);
