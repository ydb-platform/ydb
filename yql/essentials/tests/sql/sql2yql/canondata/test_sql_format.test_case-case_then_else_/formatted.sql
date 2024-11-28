SELECT
    CASE
        WHEN key != subkey
            THEN subkey
        ELSE value
    END
FROM plato.Input;
