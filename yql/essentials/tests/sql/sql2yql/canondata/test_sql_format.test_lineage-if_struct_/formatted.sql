INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        IF(key == "foo", CombineMembers(RemoveMembers(LAG(data) OVER w, ["key"]), ChooseMembers(data, ["key"])), data)
    FROM (
        SELECT
            TableRow() AS data,
            key,
            value
        FROM
            plato.Input
    )
    WINDOW
        w AS (
            PARTITION BY
                key
        )
)
    FLATTEN COLUMNS
;
