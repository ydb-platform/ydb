USE plato;
PRAGMA directread;

$in = (
    SELECT
        *
    FROM
        Input
    WHERE
        a != 0
);

SELECT
    *
FROM
    $in
;
