use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeForce="true";
pragma yt.JoinMergeUnsortedFactor="0";

select a.key as key from InputSorted as a join Input as b using(key) order by key;
