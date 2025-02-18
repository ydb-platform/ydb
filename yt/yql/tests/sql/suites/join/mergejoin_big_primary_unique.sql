PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinAllowColumnRenames="true";
pragma yt.JoinMergeUseSmallAsPrimary="false";

-- Input2 is smaller than Input1, but Input1 has unique keys
select * from Input1 as a join Input2 as b on a.k1 = b.k2 and a.v1 = b.v2;

