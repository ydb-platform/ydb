#include "transparent.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/transparent.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

std::shared_ptr<IPortionsSelector> TTransparentSelectorConstructor::DoBuildSelector() const {
    return std::make_shared<TTransparentPortionsSelector>(GetName());
}

TConclusionStatus TTransparentSelectorConstructor::DoDeserializeFromJson(const NJson::TJsonValue& /*json*/) {
    return TConclusionStatus::Success();
}

bool TTransparentSelectorConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) {
    if (!proto.HasTransparent()) {
        return false;
    }
    return true;
}

void TTransparentSelectorConstructor::DoSerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const {
    *proto.MutableTransparent() = NKikimrSchemeOp::TCompactionSelectorConstructorContainer::TTransparentSelector();
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
