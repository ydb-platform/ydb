USE plato;

DEFINE SUBQUERY $f() AS
    SELECT
        *
    FROM Input
END DEFINE;

INSERT INTO Output WITH truncate
SELECT
    *
FROM $f();
