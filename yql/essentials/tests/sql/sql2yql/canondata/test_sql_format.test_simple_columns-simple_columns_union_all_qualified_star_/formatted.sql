/* postgres can not */
USE plato;
PRAGMA simplecolumns;

INSERT INTO @A (
    key,
    value
)
VALUES
    ('x', 1),
    ('y', 2);

INSERT INTO @B (
    key,
    value
)
VALUES
    ('y', 3),
    ('z', 4);
COMMIT;

SELECT
    A.*
FROM @A
    AS A
LEFT ONLY JOIN @B
    AS B
ON A.key == B.key
UNION ALL
SELECT
    B.*
FROM @A
    AS A
RIGHT ONLY JOIN @B
    AS B
ON A.key == B.key;
