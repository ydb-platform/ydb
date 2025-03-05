/* syntax version 1 */
/* postgres can not */
select
    FormatType(EmptyList), TypeKind(TypeHandle(EmptyList)), 
    FormatType(ParseType("EmptyList")), EmptyList(),
    FormatType(EvaluateType(EmptyListTypeHandle())),
    FormatType(EmptyDict), TypeKind(TypeHandle(EmptyDict)), 
    FormatType(ParseType("EmptyDict")), EmptyDict(),
    FormatType(EvaluateType(EmptyDictTypeHandle()));
