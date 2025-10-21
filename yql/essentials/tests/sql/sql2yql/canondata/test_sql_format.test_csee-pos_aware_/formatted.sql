/* custom error: <main>:5:13: Error: Cannot parse string value from entity (#) */
PRAGMA DebugPositions;

SELECT
    Yson::ConvertToString(d['answer']),
    Yson::ConvertToString(d['query'])
FROM (
    SELECT
        "{answer=foo;query=#}"y AS d
);
