PRAGMA EmitAggApply;

SELECT
    some(key)
FROM (
    VALUES
        (NULL)
) AS a (
    key
);
