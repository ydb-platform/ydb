SELECT
    key,
    subkey,
    CASE
        WHEN value != subkey
            THEN "WAT"
        ELSE value
    END AS value
FROM plato.Input;
