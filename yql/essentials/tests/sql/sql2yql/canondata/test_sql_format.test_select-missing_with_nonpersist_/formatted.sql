USE plato;

$data = (
    SELECT
        Yson::Parse(CAST(key AS Yson)) AS key,
        text, -- missing colums
        subkey
    FROM
        Input
);

SELECT
    key,
    subkey
FROM
    $data
;
