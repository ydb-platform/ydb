USE plato;

PRAGMA SeqMode;

INSERT INTO @foo
SELECT
    1
;

COMMIT;

$a =
    SELECT
        *
    FROM
        @foo
;

SELECT
    *
FROM
    $a
;

SELECT
    *
FROM
    $a
;
