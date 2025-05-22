/* syntax version 1 */
/* postgres can not */
select DictLength(AsSetStrict(1,2,3)),DictLength(AsSet(1,2,3u)),DictLength(SetCreate(Int32))