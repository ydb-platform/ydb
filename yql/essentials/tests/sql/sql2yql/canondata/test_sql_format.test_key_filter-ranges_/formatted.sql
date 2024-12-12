/* postgres can not */
SELECT
    *
FROM (
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key <= "037" AND key >= "037"
    UNION ALL
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key >= "037" AND key <= "037"
    UNION ALL
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key BETWEEN "037" AND "037"
)
ORDER BY
    key,
    subkey
;
