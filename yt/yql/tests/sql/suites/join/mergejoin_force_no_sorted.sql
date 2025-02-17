use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeForce="true";

select a.key as key from Input1 as a join Input2 as b using(key) order by key;
