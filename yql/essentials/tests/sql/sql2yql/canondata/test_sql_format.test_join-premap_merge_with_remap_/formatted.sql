PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3.0";

INSERT INTO @inp2
SELECT
    *
FROM
    Input2
ORDER BY
    BaseNo DESC
;
COMMIT;

SELECT
    pd.BaseNo,
    m2.PhraseID,
    m2.Extra
FROM
    Input1 AS pd
LEFT JOIN (
    SELECT
        BaseNo,
        PhraseID,
        1 AS Extra
    FROM
        @inp2
) AS m2
USING (BaseNo, PhraseID)
ORDER BY
    pd.BaseNo,
    m2.PhraseID,
    m2.Extra
;
