SELECT
    TypeKind(TypeHandle(LinearType(Int32))),
    TypeKind(TypeHandle(DynamicLinearType(Int32))),
    FormatType(EvaluateType(TypeHandle(LinearType(Int32)))),
    FormatType(LinearTypeHandle(TypeHandle(Int32))),
    FormatType(DynamicLinearTypeHandle(TypeHandle(Int32))),
    FormatType(LinearItemType(TypeHandle(LinearType(Int32))))
;
