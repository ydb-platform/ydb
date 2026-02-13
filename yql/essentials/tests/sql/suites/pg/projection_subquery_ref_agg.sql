--!syntax_pg

/* custom error: Member not found: _alias_t.x */

-- TODO(YQL-20943): bad test.

SELECT (Count(x) / (
    SELECT Count(y)
    FROM (VALUES ('1')) AS t(y)
    HAVING Count(x) = Count(y)
))
FROM (VALUES (1)) AS t(x)
GROUP BY x;
