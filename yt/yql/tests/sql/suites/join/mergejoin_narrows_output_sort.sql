use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3";
pragma yt.JoinAllowColumnRenames="true";
pragma SimpleColumns;

from Input3 as c
join Input4 as d on c.k3 = d.k4
right only join Input1 as a on a.k1 = c.k3 and a.v1 = c.v3
select * order by u1

