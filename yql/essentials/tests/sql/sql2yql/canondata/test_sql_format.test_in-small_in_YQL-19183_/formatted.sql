$a = ('x',);
$b = ('x', 'y');

SELECT
    $a IN ($a, $b)
;
