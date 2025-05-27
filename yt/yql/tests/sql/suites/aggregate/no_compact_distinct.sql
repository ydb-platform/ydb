USE plato;

pragma AnsiOptionalAs;

$x = (
    SELECT key, AVG(DISTINCT Cast(subkey as float)) s FROM InputB GROUP BY key
);

$y = (
    SELECT key, SUM(Cast(subkey as float)) s FROM InputC GROUP BY key
);

SELECT x.key, x.s AS s1, y.s AS s2 FROM $x x FULL OUTER JOIN $y y ON x.key = y.key;
