PRAGMA config.flags('OptimizerFlags', 'DisableSameBranchesCollapse');

$x = ToMutDict({1: 2}, 0);

$y = IF_STRICT(
    Opaque(TRUE),
    $x,
    $x
);

SELECT
    FromMutDict($y)
;
