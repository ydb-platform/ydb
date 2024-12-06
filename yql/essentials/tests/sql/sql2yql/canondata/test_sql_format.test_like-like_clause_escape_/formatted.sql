SELECT
    key,
    subkey,
    value
FROM plato.Input
WHERE value LIKE "!%z" ESCAPE "!"
    OR value || "_" LIKE "_?_" ESCAPE "?"
    OR value || "!" LIKE "ddd!!" ESCAPE "!";
