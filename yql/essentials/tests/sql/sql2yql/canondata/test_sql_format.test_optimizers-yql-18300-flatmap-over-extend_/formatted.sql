USE plato;

INSERT INTO @tmp WITH truncate
SELECT
    "dummy" AS a,
    "1" AS b,
    ["b", "s"] AS data
ORDER BY
    a
;
COMMIT;

SELECT
    a,
    id
FROM (
    SELECT
        a,
        ListExtend(
            [String::AsciiToLower(b)],
            ListMap(data, String::AsciiToLower)
        ) AS joins
    FROM
        @tmp
)
    FLATTEN LIST BY joins AS id
;
