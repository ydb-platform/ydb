/* syntax version 1 */
/* postgres can not */
SELECT
    TypeKind(TypeHandle(TypeOf(1))),
    TypeKind(TypeHandle(TypeOf(AsList(1)))),
    TypeKind(TypeHandle(TypeOf(NULL))),
    TypeKind(TypeHandle(TypeOf(TypeOf(1)))),
    TypeKind(TypeHandle(TypeOf(AsAtom('1'))))
;
