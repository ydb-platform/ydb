#include "snapshot.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/snapshot.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

std::shared_ptr<IPortionsSelector> TSnapshotSelectorConstructor::DoBuildSelector() const {
    return std::make_shared<TSnapshotPortionsSelector>(Interval, GetName());
}

TConclusionStatus TSnapshotSelectorConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonValue) {
    if (jsonValue.Has("interval")) {
        auto conclusion = Interval.DeserializeFromJson(jsonValue["interval"]);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

bool TSnapshotSelectorConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) {
    if (!proto.HasSnapshot()) {
        return false;
    }
    auto conclusion = Interval.DeserializeFromProto(proto.GetSnapshot().GetInterval());
    if (conclusion.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot parse snapshot selector interval")("reason", conclusion.GetErrorMessage());
        return false;
    }
    return true;
}

void TSnapshotSelectorConstructor::DoSerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const {
    *proto.MutableSnapshot()->MutableInterval() = Interval.SerializeToProto();
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
