/* postgres can not */
/* kikimr can not */
/* multirun can not */
USE plato;
pragma yt.MaxInputTables="3";
pragma yt.MaxInputTablesForSortedMerge="2";

INSERT INTO Output
SELECT
    key,
    value
FROM concat(Input, Input, Input, Input)
;