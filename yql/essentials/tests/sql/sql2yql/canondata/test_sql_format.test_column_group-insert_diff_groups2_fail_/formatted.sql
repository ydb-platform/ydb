/* custom error:Insert with different "column_groups" to existing table is not allowed*/
USE plato;
PRAGMA yt.ColumnGroupMode = "perusage";

$s1 =
    SELECT
        *
    FROM Input
    WHERE a != "";

$s2 =
    SELECT
        *
    FROM Input
    WHERE a > "a1";

INSERT INTO Output
SELECT
    *
FROM $s1;
COMMIT;

INSERT INTO Output WITH column_groups = "{a=#}"
SELECT
    *
FROM $s2;
