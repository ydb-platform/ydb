#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets {

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
    *proto.MutableLBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();
}

bool TOptimizerPlannerConstructor::DoDeserializeFromProto(const TProto& proto) {
    if (!proto.HasLBuckets()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse l-buckets optimizer from proto")("proto", proto.DebugString());
        return false;
    }
    return true;
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
