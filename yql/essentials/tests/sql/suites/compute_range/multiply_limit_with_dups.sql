/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

$lst = ListExtend(
  ListFromRange(0, 5000),
  ListFromRange(0, 5000),
  ListFromRange(5000, 10000)
);  

$lst = ListMap($lst, ($x)->(AsTuple(cast($x as String), $x)));

select YQL::RangeComputeFor(
  Struct<
    a:Int32?,
    b:String,
  >,

  ($row) -> (((
              $row.b,
              $row.a)
              IN $lst) ?? false),
  AsTuple(
    AsAtom("a"),
    AsAtom("b"),
  )
);

