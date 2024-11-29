/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.FolderInlineItemsLimit = "0";

$list = (
    SELECT
        aggregate_list(Path)
    FROM (
        SELECT
            Path
        FROM folder("")
        WHERE Type == "table"
        LIMIT 30
    )
);

SELECT
    count(*)
FROM each($list);
