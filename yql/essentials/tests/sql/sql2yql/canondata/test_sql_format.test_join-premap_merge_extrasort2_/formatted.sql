PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3.0";

SELECT
    pd.Text,
    m2.BannerID
FROM (
    SELECT
        BaseNo,
        PhraseID,
        100 AS BannerID
    FROM
        Input2
) AS m2
INNER JOIN (
    SELECT
        PhraseID,
        BaseNo,
        999 AS Text
    FROM
        Input1
) AS pd
USING (BaseNo, PhraseID)
ORDER BY
    pd.Text,
    m2.BannerID
;
