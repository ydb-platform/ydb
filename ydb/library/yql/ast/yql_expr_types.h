#pragma once
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/stream/output.h>

namespace NYql {

#define YQL_TYPE_ANN_KIND_MAP(xx) \
    xx(Unit, 1) \
    xx(Tuple, 2) \
    xx(Struct, 3) \
    xx(Item, 4) \
    xx(List, 5) \
    xx(Data, 6) \
    xx(World, 7) \
    xx(Optional, 8) \
    xx(Type, 9) \
    xx(Dict, 10) \
    xx(Void, 11) \
    xx(Callable, 12) \
    xx(Generic, 13) \
    xx(Resource, 14) \
    xx(Tagged, 15) \
    xx(Error, 16) \
    xx(Variant, 17) \
    xx(Stream, 18) \
    xx(Null, 19) \
    xx(Flow, 20) \
    xx(EmptyList, 21) \
    xx(EmptyDict, 22) \
    xx(Multi, 23) \
    xx(Pg, 24) \
    xx(Block, 25) \
    xx(Scalar, 26)

enum class ETypeAnnotationKind : ui64 {
    YQL_TYPE_ANN_KIND_MAP(ENUM_VALUE_GEN)
    LastType
};

}

template<>
void Out<NYql::ETypeAnnotationKind>(class IOutputStream &o, NYql::ETypeAnnotationKind x);
