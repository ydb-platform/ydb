SELECT
    key,
    subkey,
    CASE value
        WHEN subkey THEN "WAT"
        ELSE value
    END AS value
FROM plato.Input;
