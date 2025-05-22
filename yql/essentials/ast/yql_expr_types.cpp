#include "yql_expr_types.h"

namespace NYql {
}

template<>
void Out<NYql::ETypeAnnotationKind>(class IOutputStream &o, NYql::ETypeAnnotationKind x) {
#define YQL_TYPE_ANN_KIND_MAP_TO_STRING_IMPL(name, ...) \
    case NYql::ETypeAnnotationKind::name: \
        o << #name; \
        return;

    switch (x) {
        YQL_TYPE_ANN_KIND_MAP(YQL_TYPE_ANN_KIND_MAP_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}
