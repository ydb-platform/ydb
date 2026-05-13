#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets {

NKikimr::TConclusion<std::shared_ptr<NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(
    const TBuildContext& context) const {
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), GetNodePortionsCountLimit());
}

bool TOptimizerPlannerConstructor::DoApplyToCurrentObject(IOptimizerPlanner& current) const {
    auto* itemClass = dynamic_cast<TOptimizerPlanner*>(&current);
    if (!itemClass) {
        return false;
    }
    return true;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    *proto.MutableLBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();
}

bool TOptimizerPlannerConstructor::DoDeserializeFromProto(const TProto& proto) {
    if (!proto.HasLBuckets()) {
        YDB_LOG_ERROR("",
            {"error", "cannot parse l-buckets optimizer from proto"},
            {"proto", proto.DebugString()});
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
