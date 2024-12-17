/* postgres can not */
/* syntax version 1 */
-- compiles to different code in v0/v1 due to different SplitToList settings
SELECT
    *
FROM
    plato.Input
WHERE
    key IN String::SplitToList(FileContent('keyid.lst'), '\n', TRUE)
;
