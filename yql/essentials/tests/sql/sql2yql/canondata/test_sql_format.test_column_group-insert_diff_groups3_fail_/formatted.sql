/* custom error:Insert with "column_groups" to existing table is not allowed*/
USE plato;
PRAGMA yt.ColumnGroupMode = "perusage";

INSERT INTO Output WITH column_groups = "{a=#}"
SELECT
    *
FROM Input
WHERE a != "";
