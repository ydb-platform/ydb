/* postgres can not */
/* dq can not */
/* dqfile can not */
$s = <|foo: 1p|>;
$t = (1p,);

SELECT
    CAST($s AS Struct<a: PgText, b: PgInt4, c: _PgText, foo: PgInt4>)
;

SELECT
    CAST($t AS Tuple<PgInt4, PgText, _PgText>)
;
