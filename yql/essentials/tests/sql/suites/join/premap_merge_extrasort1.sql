PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";


SELECT pd.Text, pd.PhraseID, m2.BaseNo
    FROM Input2 as m2
    INNER JOIN (select PhraseID, BaseNo, True as Text from Input1) as pd
    USING (BaseNo, PhraseID)
ORDER BY pd.PhraseID, m2.BaseNo;
