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

INSERT INTO @a WITH column_groups = "{a=#}"
SELECT
    *
FROM $s1;

INSERT INTO @b
SELECT
    *
FROM $s1;

INSERT INTO @c
SELECT
    *
FROM $s1;

INSERT INTO Output WITH column_groups = "{a=#}"
SELECT
    *
FROM $s1;
COMMIT;

INSERT INTO @a WITH column_groups = "{a=#}"
SELECT
    *
FROM $s2;

INSERT INTO @b
SELECT
    *
FROM $s2;

INSERT INTO @c WITH column_groups = "{default=#}"
SELECT
    *
FROM $s2;

INSERT INTO Output WITH column_groups = "{a=#}"
SELECT
    *
FROM $s2;
