#include "empty.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/empty.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

std::shared_ptr<IPortionsSelector> TEmptySelectorConstructor::DoBuildSelector() const {
    return std::make_shared<TEmptyPortionsSelector>(GetName());
}

TConclusionStatus TEmptySelectorConstructor::DoDeserializeFromJson(const NJson::TJsonValue& /*json*/) {
    return TConclusionStatus::Success();
}

bool TEmptySelectorConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) {
    if (!proto.HasEmpty()) {
        return false;
    }
    return true;
}

void TEmptySelectorConstructor::DoSerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const {
    *proto.MutableEmpty() = NKikimrSchemeOp::TCompactionSelectorConstructorContainer::TEmptySelector();
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
