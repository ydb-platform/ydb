PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";

SELECT pd.Text
    FROM Input2 as m2
    INNER JOIN Input1 as pd
    USING (BaseNo, PhraseID)
ORDER BY pd.Text;
