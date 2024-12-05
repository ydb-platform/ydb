SELECT
    key,
    subkey,
    CASE value
        WHEN "jar" THEN "JAR"
        WHEN "foo" THEN "FOO"
        ELSE value
    END AS value
FROM plato.Input;
