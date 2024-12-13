SELECT
    *
FROM (
    SELECT
        "x023" AS key,
        subkey,
        value
    FROM
        plato.Input
    WHERE
        key == "023"
    UNION ALL
    SELECT
        "x037" AS key,
        subkey,
        value
    FROM
        plato.Input
    WHERE
        key == "037"
    UNION ALL
    SELECT
        "x075" AS key,
        subkey,
        value
    FROM
        plato.Input
    WHERE
        key == "075"
    UNION ALL
    SELECT
        "x150" AS key,
        subkey,
        value
    FROM
        plato.Input
    WHERE
        key == "150"
) AS x
ORDER BY
    key,
    subkey
;
