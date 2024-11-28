USE plato;

$key =
    SELECT
        min(key)
    FROM Input;

INSERT INTO Output
SELECT
    $key AS key;
