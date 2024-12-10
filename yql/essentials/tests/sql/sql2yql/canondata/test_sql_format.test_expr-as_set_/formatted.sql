/* syntax version 1 */
/* postgres can not */
SELECT
    DictLength(AsSetStrict(1, 2, 3)),
    DictLength(AsSet(1, 2, 3u)),
    DictLength(SetCreate(Int32))
;
