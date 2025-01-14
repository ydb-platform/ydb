use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="2";

select * from Input where key in (select "023" as key union all select "911" as key union all select "911" as key);
