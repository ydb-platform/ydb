SELECT
    *
FROM
    plato.Input
WHERE
    key > "075" OR (key == "075" AND subkey == "1" AND value == "abc")
ORDER BY
    key,
    subkey,
    value
;
