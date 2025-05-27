/* syntax version 1 */
/* postgres can not */
SELECT
    Yql::Append(AsList(ListCreate(Int32)), []),
    Yql::Append(AsList(DictCreate(Int32, String)), {}),
    CAST([] AS List<Int32>),
    CAST({} AS Dict<Int32, String>),
    CAST({} AS Set<Int32>),
    AsList(ListCreate(Int32), []),
    AsList([], ListCreate(Int32)),
    AsList(DictCreate(Int32, String), {}),
    AsList({}, DictCreate(Int32, String)),
    AsList(SetCreate(Int32), {}),
    AsList({}, SetCreate(Int32))
;
