USE plato;
PRAGMA AnsiInForEmptyOrNullableItemsCollections = "true";
PRAGMA yt.MapJoinLimit = "1m";
PRAGMA yt.LookupJoinLimit = "64k";
PRAGMA yt.LookupJoinMaxRows = "100";

INSERT INTO @T1
SELECT
    Just('fooo'u) AS ID
ORDER BY
    ID
;

INSERT INTO @T2
SELECT
    't' AS text,
    '{}'y AS tags,
    'foo' AS ID
ORDER BY
    ID
;
COMMIT;

$lost_ids =
    SELECT
        ID
    FROM
        @T2
    WHERE
        ID NOT IN (
            SELECT
                ID
            FROM
                @T1
        )
;

$lost_samples_after_align =
    SELECT
        *
    FROM
        @T2
    WHERE
        ID IN $lost_ids
;

SELECT
    *
FROM
    $lost_samples_after_align
;

SELECT
    text || 'a' AS text,
    tags,
FROM
    $lost_samples_after_align
;
