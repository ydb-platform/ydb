SELECT
    *
FROM
    plato.Input
WHERE
    key IS NULL AND subkey >= "0" AND subkey <= "9"
;
