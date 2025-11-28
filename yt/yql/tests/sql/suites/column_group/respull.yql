-- YQL-19570
-- Expected no column_group in YtMap outputs
USE plato;

pragma yt.ColumnGroupMode="perusage";
pragma yt.OptimizeFor="lookup";

SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM Input
WHERE key < "050"
LIMIT 1;
