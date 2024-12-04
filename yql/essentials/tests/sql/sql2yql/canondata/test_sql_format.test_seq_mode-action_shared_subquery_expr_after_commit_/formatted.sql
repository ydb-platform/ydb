PRAGMA SeqMode;
USE plato;

DEFINE ACTION $a() AS
    INSERT INTO @tmp
    SELECT
        1;
    COMMIT;

    $r =
        SELECT
            *
        FROM @tmp;

    SELECT
        *
    FROM $r;

    SELECT
        *
    FROM $r;
END DEFINE;
DO $a();
