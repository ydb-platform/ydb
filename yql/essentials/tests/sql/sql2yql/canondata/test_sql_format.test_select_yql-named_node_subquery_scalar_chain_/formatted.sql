PRAGMA YqlSelect = 'force';

$x = (
    SELECT
        1
);

$y = $x;
$z = $y;

SELECT
    $x,
    $y,
    $z
;
