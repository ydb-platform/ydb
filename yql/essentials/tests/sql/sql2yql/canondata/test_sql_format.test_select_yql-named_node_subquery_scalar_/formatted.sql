PRAGMA YqlSelect = 'force';

$x = (
    SELECT
        1
);

$y = (
    SELECT
        1
);

SELECT
    $x,
    $y
;
