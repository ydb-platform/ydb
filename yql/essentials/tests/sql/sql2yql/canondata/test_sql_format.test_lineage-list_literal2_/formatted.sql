USE plato;

$key =
    SELECT
        min(key)
    FROM
        Input
;

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        $key AS key
)
WHERE
    key > ''
;
