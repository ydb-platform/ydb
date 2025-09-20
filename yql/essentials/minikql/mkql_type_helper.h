#pragma once

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

inline TType* SkipTaggedType(TType* type) {
    while (type->IsTagged()) {
        type = AS_TYPE(TTaggedType, type)->GetBaseType();
    }
    return type;
}

inline bool IsSingularType(const TType* type) {
    return type->IsNull() ||
           type->IsVoid() ||
           type->IsEmptyDict() ||
           type->IsEmptyList();
}

inline bool NeedWrapWithExternalOptional(TType* type) {
    type = SkipTaggedType(type);
    bool isOptional;
    auto unpacked = SkipTaggedType(UnpackOptional(type, isOptional));
    if (!isOptional) {
        return false;
    } else if (unpacked->IsOptional()) {
        return true;
    } else if (unpacked->IsPg() || IsSingularType(unpacked)) {
        return true;
    }
    return false;
}

} // namespace NKikimr::NMiniKQL
