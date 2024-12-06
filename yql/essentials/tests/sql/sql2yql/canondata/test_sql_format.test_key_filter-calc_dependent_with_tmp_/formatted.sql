/* postgres can not */
USE plato;

INSERT INTO @temp
SELECT
    *
FROM Input
ORDER BY
    key DESC
LIMIT 1;
COMMIT;

$last_key =
    SELECT
        key
    FROM @temp
    LIMIT 1;

SELECT
    *
FROM Input
WHERE key == $last_key;
