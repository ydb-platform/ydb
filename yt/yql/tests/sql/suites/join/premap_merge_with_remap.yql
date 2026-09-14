PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";

INSERT INTO @inp2
SELECT * FROM Input2
ORDER BY BaseNo DESC;

COMMIT;

SELECT pd.BaseNo, m2.PhraseID, m2.Extra
    FROM Input1 as pd
    LEFT JOIN (select BaseNo, PhraseID, 1 as Extra from @inp2) as m2
    USING (BaseNo, PhraseID)
ORDER BY pd.BaseNo, m2.PhraseID, m2.Extra;
