/* postgres can not */
/* syntax version 1 */
USE plato;

$shiftSteps = 1;
$linear = ($x, $z) -> {
  $v = 10 * $z + $x;
  $shift = ($item) -> {
      return $item << $shiftSteps
  };
  $res = Math::Floor(Math::Pi() * $shift($v));
  return $res
};

--INSERT INTO Output
SELECT t.*, $linear(cast(key as uint64), cast(subkey as uint64)) as linear FROM Input as t;
