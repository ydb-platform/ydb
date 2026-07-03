PRAGMA EmitAggApply;

SELECT
    sum(key)
FROM (
    VALUES
        (1),
        (NULL),
        (3)
) AS a (
    key
);
