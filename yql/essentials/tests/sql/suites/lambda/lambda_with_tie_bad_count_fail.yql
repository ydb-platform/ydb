/* custom error:Expected tuple type of size: 2, but got: 3*/
$func = ($x, $y)->{
  $y, $x = AsTuple($x, $y, $x);
  return $x || "_" || $y;
};

SELECT $func('foo', 'bar');

