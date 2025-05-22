/* syntax version 1 */
/* postgres can not */
USE plato;

define action $action1($x) as
  select $x;
end define;

$f = ($i)->{
   return CAST(Unicode::ToUpper(cast($i as Utf8)) AS String);
};

evaluate for $i in ListMap(ListFromRange(0,3),$f) do $action1($i);

evaluate for $i in ListMap(ListFromRange(0,0),$f) do $action1($i) else do $action1(100);

evaluate for $i in ListMap(ListFromRange(0,0),$f) do $action1($i);

evaluate for $i in Yql::Map(1/1,($x)->{return AsList($x)}) do $action1($i);

evaluate for $i in Yql::Map(1/0,($x)->{return AsList($x)}) do $action1($i);
