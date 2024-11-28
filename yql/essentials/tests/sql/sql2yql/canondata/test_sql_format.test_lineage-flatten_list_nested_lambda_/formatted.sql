USE plato;

$sub = (
    SELECT
        key
    FROM Input
        FLATTEN LIST BY
            key
);

INSERT INTO Output
SELECT
    value,
    ListFilter(
        [value],
        ($x) -> ($x IN $sub)
    ) AS f
FROM Input
ORDER BY
    value;
