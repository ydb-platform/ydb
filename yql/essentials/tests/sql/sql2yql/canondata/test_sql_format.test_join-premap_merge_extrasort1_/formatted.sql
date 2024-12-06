PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3.0";

SELECT
    pd.Text,
    pd.PhraseID,
    m2.BaseNo
FROM
    Input2 AS m2
INNER JOIN (
    SELECT
        PhraseID,
        BaseNo,
        TRUE AS Text
    FROM
        Input1
) AS pd
USING (BaseNo, PhraseID)
ORDER BY
    pd.PhraseID,
    m2.BaseNo
;
