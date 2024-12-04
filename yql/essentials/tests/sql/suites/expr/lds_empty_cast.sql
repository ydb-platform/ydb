/* syntax version 1 */
/* postgres can not */
select
    Yql::Append(AsList(ListCreate(Int32)), []),
    Yql::Append(AsList(DictCreate(Int32, String)), {}),
    cast([] as List<Int32>),
    cast({} as Dict<Int32, String>),
    cast({} as Set<Int32>),
    AsList(ListCreate(Int32),[]),
    AsList([],ListCreate(Int32)),
    AsList(DictCreate(Int32, String),{}),
    AsList({},DictCreate(Int32, String)),
    AsList(SetCreate(Int32),{}),
    AsList({},SetCreate(Int32));
