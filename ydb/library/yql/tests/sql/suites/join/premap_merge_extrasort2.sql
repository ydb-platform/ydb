PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";


SELECT pd.Text, m2.BannerID
    FROM (select BaseNo, PhraseID, 100 as BannerID from Input2) as m2
    INNER JOIN (select PhraseID, BaseNo, 999 as Text from Input1) as pd
    USING (BaseNo, PhraseID)
ORDER BY pd.Text, m2.BannerID;
