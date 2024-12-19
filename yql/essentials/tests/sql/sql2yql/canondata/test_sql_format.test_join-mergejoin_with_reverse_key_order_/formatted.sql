PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeUnsortedFactor = '3.0';

SELECT
    pd.Text
FROM
    Input2 AS m2
INNER JOIN
    Input1 AS pd
USING (BaseNo, PhraseID)
ORDER BY
    pd.Text
;
