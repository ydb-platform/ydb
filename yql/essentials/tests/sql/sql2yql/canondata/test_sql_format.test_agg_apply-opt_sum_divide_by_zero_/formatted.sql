PRAGMA EmitAggApply;

SELECT
    sum(1 / 0)
FROM (
    VALUES
        (1),
        (NULL),
        (3)
) AS a (
    key
);
