/* postgres can not */
USE plato;
PRAGMA DisableSimpleColumns;

$shiftSteps=($item) -> { return Cast($item % 4 as Uint8)??0 };

$linear = ($x, $z, $func) -> {
  $v = 10 * $z + $x;
  $shift = ($item, $sk) -> {return $item << $func($sk)};
  return $shift($v, $z)
};

--INSERT INTO Output
SELECT t.*, $linear(cast(key as uint64), cast(subkey as uint64), $shiftSteps) FROM Input as t;
