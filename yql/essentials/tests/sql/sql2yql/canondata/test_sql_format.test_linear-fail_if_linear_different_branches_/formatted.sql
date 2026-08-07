/* custom error: Cannot guarantee single use in static analysis, consider using DynamicLinear */
$x = WithSideEffects(ToMutDict({1: 2}, NULL));
$z = WithSideEffects(ToMutDict({3: 4}, NULL));

$y = IF(
    Opaque(TRUE),
    ($x, 1),
    ($z, 2)
);

SELECT
    FromMutDict($y.0),
    $y.1
;
