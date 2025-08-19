/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;
pragma warning("disable", "4510");

$x1 = ()->(Int32);
$x2 = ()->(Tuple<Int32>);
$y1 = ()->{$a = String;return $a};
$y2 = ()->{$a = Tuple<String>;return $a};
$z1 = ()->{return Double};
$z2 = ()->{return Tuple<Double>};

select
  timestamp,
  EnsureType(ts,timestamp),
  FormatType(timestamp),
  FormatType(TypeOf(timestamp)),
  FormatType(TypeOf(InstanceOf(timestamp))),
  Nothing(OptionalType(timestamp)),
  FormatType(OptionalType(timestamp)),
  FormatType(ListType(timestamp)),
  FormatType(StreamType(timeStamp)),
  FormatType(DictType(timestamp,timestamp)),
  FormatType(TupleType(timestamp,int32)),
  FormatType(TaggedType(timestamp,'foo')),
  FormatType(CallableType(0,timestamp)),
  Yql::Minus(timestamp),
  FormatType(Yql::OptionalType(timestamp)),
  FormatType(TypeHandle(timestamp)),
  FormatType($x1()),
  FormatType($x2()),
  FormatType($y1()),
  FormatType($y2()),
  FormatType($z1()),
  FormatType($z2()),
from (select 1 as timestamp, timestamp('2001-01-01T00:00:00Z') as ts)
