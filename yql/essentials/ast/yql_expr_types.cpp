#include "yql_expr_types.h"

namespace NYql {
}

template <>
void Out<NYql::ETypeAnnotationKind>(class IOutputStream& out, NYql::ETypeAnnotationKind value) {
#define YQL_TYPE_ANN_KIND_MAP_TO_STRING_IMPL(name, ...) \
    case NYql::ETypeAnnotationKind::name:               \
        out << #name;                                   \
        return;

    switch (value) {
        YQL_TYPE_ANN_KIND_MAP(YQL_TYPE_ANN_KIND_MAP_TO_STRING_IMPL)
        default:
            out << static_cast<int>(value);
            return;
    }
}
