SELECT
    *
FROM
    plato.Input
WHERE
    key IN ("023", "075", "150") AND (subkey == "1" OR subkey == "3") AND value >= "aaa" AND value <= "zzz"
ORDER BY
    key,
    subkey
;
