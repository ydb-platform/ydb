PRAGMA CheckedOps = 'true';

SELECT
    sum(NULL),
    sum(x),
    sumif(x, TRUE),
    sum(just(x)),
    sumif(just(x), TRUE)
FROM (
    VALUES
        (18446744073709551615ul),
        (2ul)
) AS a (
    x
);
