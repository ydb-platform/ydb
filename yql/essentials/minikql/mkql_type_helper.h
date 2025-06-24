#pragma once

#include <yql/essentials/minikql/mkql_node_builder.h>

namespace NKikimr::NMiniKQL {

inline bool IsSingularType(const TType* type) {
    return type->IsNull() ||
           type->IsVoid() ||
           type->IsEmptyDict() ||
           type->IsEmptyList();
}

inline bool NeedWrapWithExternalOptional(TType* type) {
    bool isOptional;
    auto unpacked = UnpackOptional(type, isOptional);
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
