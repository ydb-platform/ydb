SELECT
    CASE value
        WHEN key THEN subkey
        ELSE value
    END
FROM
    plato.Input
;
