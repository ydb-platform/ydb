/* postgres can not */
USE plato;

$func = ($x, $y)->{
  $y, $x = AsTuple($x, $y, $x);
  return $x || "_" || $y;
};

--INSERT INTO Output
SELECT $func(key, subkey) as func FROM Input;
