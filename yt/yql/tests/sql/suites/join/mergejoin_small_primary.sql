PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinAllowColumnRenames="true";
pragma yt.JoinMergeUseSmallAsPrimary="true";

-- Input2 is smaller than Input1
select * from Input2 as b join Input1 as a on a.k1 = b.k2
order by a.v1, b.v2;

