PRAGMA YqlSelect = 'force';

$x = (
    SELECT
        1
);

$y = $x;
$z = $y;

SELECT
    1 IN $x,
    1 IN $y,
    1 IN $z,
;
