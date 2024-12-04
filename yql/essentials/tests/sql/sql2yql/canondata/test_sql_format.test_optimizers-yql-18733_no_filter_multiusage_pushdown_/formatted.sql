PRAGMA config.flags("OptimizerFlags", "FilterPushdownEnableMultiusage");
USE plato;

$src =
    SELECT DISTINCT
        key
    FROM Input
    WHERE value == 'ddd';

SELECT
    *
FROM Input
WHERE key == $src;
