#pragma once

#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/defs.h>

#include <vector>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

inline bool UnwrapBlockTypes(const TArrayRef<TType* const>& typeComponents, std::vector<TType*>& result)
{
    bool hasBlock = false;
    bool hasNonBlock = false;

    result.reserve(typeComponents.size());
    for (TType* type : typeComponents) {
        if (type->GetKind() == TType::EKind::Block) {
            hasBlock = true;
            type = static_cast<const TBlockType*>(type)->GetItemType();
        } else {
            hasNonBlock = true;
        }
        result.push_back(type);
    }
    MKQL_ENSURE(hasBlock != hasNonBlock, "Inconsistent wide item types: mixing of blocks and non-blocks detected");
    return hasBlock;
};

inline void WrapArrayBlockTypes(std::vector<TType*>& types, const TProgramBuilder& pb)
{
    std::transform(
        types.begin(),
        types.end(),
        types.begin(),
        [&](TType* type) {
            return pb.NewBlockType(type, TBlockType::EShape::Many);
        }
    );
}

}
}