/* postgres can not */
/* syntax version 1 */
-- compiles to different code in v0/v1 due to different SplitToList settings
select * from plato.Input where key in String::SplitToList(FileContent("keyid.lst"), "\n", true);
