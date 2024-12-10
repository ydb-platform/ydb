SELECT
    EXISTS (
        SELECT
            *
        FROM
            plato.Input
        WHERE
            key == 'none'
    )
FROM
    plato.Input
;
