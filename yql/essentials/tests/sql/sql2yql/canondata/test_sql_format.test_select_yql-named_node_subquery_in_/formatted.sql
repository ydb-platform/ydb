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
    1 IN $x,
    1 IN $y
;
