pragma warning("disable","1108");
$a = ('x',);
$b = ('x','y');

SELECT  $a in ($a, $b);

