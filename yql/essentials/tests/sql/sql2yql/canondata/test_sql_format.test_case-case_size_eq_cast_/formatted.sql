SELECT
    key,
    subkey,
    CASE length(value)
        WHEN CAST(3 AS smallint) THEN "JAR"
        ELSE value
    END AS value
FROM
    plato.Input
;
