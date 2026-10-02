PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinAllowColumnRenames="true";
pragma yt.JoinMergeUseSmallAsPrimary="true";
pragma yt.JoinWaitAllInputs="true";

INSERT INTO @Input2Sorted SELECT * FROM Input2 ORDER BY key;
COMMIT;

-- Input1 is smaller than Input2 (known thanks to JoinWaitAllInputs)
select * from @Input2Sorted as b join /*+ merge() */ Input1 as a on a.k1 = b.key
order by a.v1, b.value;
