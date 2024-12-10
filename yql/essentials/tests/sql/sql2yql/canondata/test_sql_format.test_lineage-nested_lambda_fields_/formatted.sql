USE plato;

$v = ($x) -> {
    RETURN ListFilter(["a", "b"], ($y) -> ($y == $x));
};

$k = ($x) -> {
    RETURN $x;
};

INSERT INTO Output WITH truncate
SELECT
    $k(key) AS k,
    $v(value) AS v
FROM
    Input
;
