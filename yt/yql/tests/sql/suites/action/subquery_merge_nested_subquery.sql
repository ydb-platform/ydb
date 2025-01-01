/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE SUBQUERY $get_tables_list($dir) AS
        SELECT
            Unwrap($dir || "/" || CAST(TableName(Path, "yt") AS String)) AS Path,
        FROM FOLDER($dir)
END DEFINE;

DEFINE SUBQUERY $get_all_tables_list($dirs) AS
        $get_src_tables = SubqueryExtendFor(UNWRAP(ListUniq($dirs)), $get_tables_list);
        select * from $get_src_tables();
END DEFINE;
    
process $get_all_tables_list([""]);
