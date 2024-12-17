$es = YQL::Nothing(YQL::OptionalType(YQL::DataType(AsAtom("String"))));
$eu = YQL::Nothing(YQL::OptionalType(YQL::DataType(AsAtom("Utf8"))));
$u1 = YQL::Unwrap(cast("a" as utf8));
$u2 = YQL::Unwrap(cast("b" as utf8));
----------
select YQL::Concat("a", "b");
select YQL::Concat(YQL::Just("a"), "b");
select YQL::Concat("a", YQL::Just("b"));
select YQL::Concat(YQL::Just("a"), "b");
select YQL::Concat($es, "b");
select YQL::Concat("a", $es);
select YQL::Concat($es, $es);
----------
select YQL::Concat("a", $u2);
select YQL::Concat(YQL::Just("a"), $u2);
select YQL::Concat("a", YQL::Just($u2));
select YQL::Concat(YQL::Just("a"), $u2);
select YQL::Concat($es, $u2);
select YQL::Concat("a", $eu);
select YQL::Concat($es, $eu);
----------
select YQL::Concat($u1, "b");
select YQL::Concat(YQL::Just($u1), "b");
select YQL::Concat($u1, YQL::Just("b"));
select YQL::Concat(YQL::Just($u1), "b");
select YQL::Concat($eu, "b");
select YQL::Concat($u1, $es);
select YQL::Concat($eu, $es);
----------
select YQL::Concat($u1, $u2);
select YQL::Concat(YQL::Just($u1), $u2);
select YQL::Concat($u1, YQL::Just($u2));
select YQL::Concat(YQL::Just($u1), $u2);
select YQL::Concat($eu, $u2);
select YQL::Concat($u1, $eu);
select YQL::Concat($eu, $eu);
