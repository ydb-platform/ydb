#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

NKikimr::TConclusion<std::shared_ptr<NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(const TBuildContext& context) const {
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema());
}

bool TOptimizerPlannerConstructor::DoApplyToCurrentObject(IOptimizerPlanner& current) const {
    auto* itemClass = dynamic_cast<TOptimizerPlanner*>(&current);
    if (!itemClass) {
        return false;
    }
    return true;
}

bool TOptimizerPlannerConstructor::DoIsEqualTo(const IOptimizerPlannerConstructor& item) const {
    const auto* itemClass = dynamic_cast<const TOptimizerPlannerConstructor*>(&item);
    AFL_VERIFY(itemClass);
    return true;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    *proto.MutableLCBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLCOptimizer();
}

bool TOptimizerPlannerConstructor::DoDeserializeFromProto(const TProto& proto) {
    if (!proto.HasLCBuckets()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse lc-buckets optimizer from proto")("proto", proto.DebugString());
        return false;
    }
    return true;
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
