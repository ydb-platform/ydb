/* postgres can not */
/* syntax version 1 */
DEFINE SUBQUERY $src() AS
    SELECT
        *
    FROM
        plato.Input
    ORDER BY
        subkey
    ;
END DEFINE;

DEFINE SUBQUERY $src_non_yt() AS
    SELECT
        *
    FROM
        as_table([<|key: 1, subkey: 1|>, <|key: 2, subkey: 2|>])
    ORDER BY
        subkey
    ;
END DEFINE;

SELECT
    *
FROM
    $src()
ORDER BY
    key
;

SELECT
    *
FROM
    $src_non_yt()
ORDER BY
    key
;
