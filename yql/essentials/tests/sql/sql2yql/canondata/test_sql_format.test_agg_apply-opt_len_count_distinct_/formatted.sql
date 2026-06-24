PRAGMA EmitAggApply;

SELECT
    count(DISTINCT key)
FROM (
    VALUES
        (1),
        (NULL),
        (3)
) AS a (
    key
);
