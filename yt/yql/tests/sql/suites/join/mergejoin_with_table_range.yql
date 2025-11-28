PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";

SELECT *
    FROM CONCAT("Left1", "Left2") as a
    INNER JOIN CONCAT("Right1", "Right2") as b
    USING(key)
ORDER BY a.value,b.value;

