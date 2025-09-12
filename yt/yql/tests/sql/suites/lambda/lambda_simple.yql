/* postgres can not */
USE plato;
PRAGMA DisableSimpleColumns;

$shiftSteps=1;
$linear = ($x, $z)->{
  $v = 10 * $z + $x;
  $shift = ($item) -> {return $item << $shiftSteps};
  return $shift($v)
};

--INSERT INTO Output
SELECT t.*, $linear(cast(key as uint64), cast(subkey as uint64)) FROM Input as t;
