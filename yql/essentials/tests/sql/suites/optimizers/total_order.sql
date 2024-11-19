/* postgres can not */
$x = AsTuple(Double("nan"),42);

select $x = $x;
select $x < $x;
select $x <= $x;
select $x > $x;
select $x >= $x;
select $x != $x;

$x = AsStruct(Double("nan") as a,42 as b);
select $x = $x;
select $x != $x;

$x = AsTuple(Nothing(ParseType("Int32?")), 1);
select $x = $x;
select $x < $x;
select $x <= $x;
select $x > $x;
select $x >= $x;
select $x != $x;

$x = Nothing(ParseType("Int32?"));
select $x = $x;
select $x < $x;
select $x <= $x;
select $x > $x;
select $x >= $x;
select $x != $x;
