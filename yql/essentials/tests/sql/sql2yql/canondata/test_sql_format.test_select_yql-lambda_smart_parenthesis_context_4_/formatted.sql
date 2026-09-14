PRAGMA YqlSelect = 'force';

$f = ($x) -> {
    RETURN $x;
};

SELECT
    $f(1)
;
