/* postgres can not */
$optDuoFloat=Just(Just(cast(2.71 as float)));
$optDuoDouble=Just(Just(cast(3.14 as double)));
$optDuoFloatN=Just(Just(cast(null as float)));
$optDuoDoubleN=Just(Just(cast(null as double)));

select
  ($optDuoFloat ?? 0) ?? 1,
  ($optDuoDouble ?? 41) ?? 42,
  ($optDuoFloatN ?? 0) ?? 1.,
  ($optDuoDoubleN ?? 41.) ?? 42,
  ($optDuoFloatN ?? cast(40.1 as float)) ?? cast(40.2 as float),
  ($optDuoDoubleN ?? cast(40.1 as float)) ?? cast(40.2 as float),
  (($optDuoFloatN ?? 0) ?? 1.) ?? 3,
  (($optDuoDoubleN ?? 41) ?? 42) ?? 4,
  'end'
from plato.Input;
