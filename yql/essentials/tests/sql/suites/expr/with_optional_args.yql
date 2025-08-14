/* syntax version 1 */
/* postgres can not */
$f = Yql::WithOptionalArgs(($x,$y,$z)->($x + ($y ?? 0) + ($z ?? 0)), AsAtom("2"));
select $f(1),$f(2,3),$f(4,5,6),
  Yql::NamedApply($f,(1,),<||>),
  Yql::NamedApply($f,(1,2),<||>),
  Yql::NamedApply($f,(1,2,3),<||>),
  Yql::NamedApply($f,(1,2,3,4),<||>);
