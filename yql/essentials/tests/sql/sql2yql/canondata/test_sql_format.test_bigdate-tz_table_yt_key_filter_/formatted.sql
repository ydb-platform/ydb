/* postgres can not */
/* multirun can not */
USE plato;
PRAGMA yt.UseNewPredicateExtraction;

INSERT INTO OutTzDate32
SELECT
    *
FROM as_table(AsList(
    <|key: TzDate32('-144169-1-1,UTC')|>,
    <|key: TzDate32('148107-12-31,UTC')|>
))
ORDER BY
    key;

INSERT INTO OutTzDatetime64
SELECT
    *
FROM as_table(AsList(
    <|key: TzDatetime64('-144169-1-1T0:0:0,UTC')|>,
    <|key: TzDatetime64('148107-12-31T23:59:59,UTC')|>
))
ORDER BY
    key;

INSERT INTO OutTzTimestamp64
SELECT
    *
FROM as_table(AsList(
    <|key: TzTimestamp64('-144169-1-1T0:0:0,UTC')|>,
    <|key: TzTimestamp64('148107-12-31T23:59:59.999999,UTC')|>
))
ORDER BY
    key;
COMMIT;

SELECT
    *
FROM OutTzDate32
WHERE key > TzDate32('-144169-1-1,UTC')
    AND key > TzDatetime64('-144169-1-1T0:0:0,UTC')
    AND key > TzTimestamp64('-144169-1-1T0:0:0,UTC')
    AND key >= TzDate32('148107-12-31,UTC')
    AND key >= TzDatetime64('148107-12-31T0:0:0,UTC')
    AND key >= TzTimestamp64('148107-12-31T0:0:0,UTC');

SELECT
    *
FROM OutTzDate32
WHERE key < TzDate32('148107-12-31,UTC')
    AND key < TzDatetime64('148107-12-31T23:59:59,UTC')
    AND key < TzTimestamp64('148107-12-31T23:59:59.999999,UTC')
    AND key <= TzDate32('-144169-1-1,UTC')
    AND key <= TzDatetime64('-144169-1-1T0:0:0,UTC')
    AND key <= TzTimestamp64('-144169-1-1T0:0:0,UTC');

SELECT
    *
FROM OutTzDatetime64
WHERE key > TzDatetime64('-144169-1-1T0:0:0,UTC')
    AND key > TzTimestamp64('-144169-1-1T0:0:0,UTC')
    AND key >= TzDatetime64('148107-12-31T0:0:0,UTC')
    AND key >= TzTimestamp64('148107-12-31T0:0:0,UTC');

SELECT
    *
FROM OutTzDatetime64
WHERE key < TzDatetime64('148107-12-31T23:59:59,UTC')
    AND key < TzTimestamp64('148107-12-31T23:59:59.999999,UTC')
    AND key <= TzDatetime64('-144169-1-1T0:0:0,UTC')
    AND key <= TzTimestamp64('-144169-1-1T0:0:0,UTC');

SELECT
    *
FROM OutTzTimestamp64
WHERE key > TzTimestamp64('-144169-1-1T0:0:0,UTC')
    AND key >= TzTimestamp64('148107-12-31T0:0:0,UTC');

SELECT
    *
FROM OutTzTimestamp64
WHERE key < TzTimestamp64('148107-12-31T23:59:59.999999,UTC')
    AND key <= TzTimestamp64('-144169-1-1T0:0:0,UTC');
