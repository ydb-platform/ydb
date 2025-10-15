#include "type_utils.h"

namespace NKikimr::NMiniKQL {
bool UnwrapBlockTypes(const TArrayRef<TType* const>& typeComponents, std::vector<TType*>& result) {
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
}

void WrapArrayBlockTypes(std::vector<TType*>& types, const TProgramBuilder& pb) {
    std::transform(types.begin(), types.end(), types.begin(),
                   [&](TType* type) { return pb.NewBlockType(type, TBlockType::EShape::Many); });
}

int ArrowScalarAsInt(const TArrowBlock& scalar) {
    return scalar.GetDatum().scalar_as<arrow::UInt64Scalar>().value;
}

bool ForceRightOptional(EJoinKind kind) {
    switch (kind) {
    case EJoinKind::Left:
    case EJoinKind::Exclusion:
    case EJoinKind::Full:
        return true;
    default:
        return false;
    }
}

bool ForceLeftOptional(EJoinKind kind) {
    switch (kind) {
    case EJoinKind::Right:
    case EJoinKind::Exclusion:
    case EJoinKind::Full:
        return true;
    default:
        return false;
    }
}
} // namespace NKikimr::NMiniKQL