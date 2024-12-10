USE plato;

SELECT
    *
FROM (
    SELECT
        key
    FROM
        Input
) AS a
WHERE
    key >= "030" AND key <= "200"
;
