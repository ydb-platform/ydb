#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/optimizer/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/one_head/logic.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/slices/logic.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

std::shared_ptr<IOptimizationLogic> TOptimizerPlannerConstructor::BuildLogic() const {
    const TDuration freshnessCheckDuration = NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration();
    std::shared_ptr<IOptimizationLogic> logic;
    if (LogicName == "one_head") {
        logic = std::make_shared<TOneHeadLogic>(freshnessCheckDuration);
    } else if (LogicName == "slices") {
        logic = std::make_shared<TTimeSliceLogic>(freshnessCheckDuration);
    } else {
        AFL_VERIFY(false)("ln", LogicName);
    }
    return logic;
}

TConclusion<std::shared_ptr<NOlap::NStorageOptimizer::IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(const TBuildContext& context) const {
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), BuildLogic());
}

bool TOptimizerPlannerConstructor::DoIsEqualTo(const IOptimizerPlannerConstructor& item) const {
    const auto* itemClass = dynamic_cast<const TOptimizerPlannerConstructor*>(&item);
    AFL_VERIFY(itemClass);
    return LogicName == itemClass->LogicName && FreshnessCheckDuration == itemClass->FreshnessCheckDuration;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    proto.MutableSBuckets()->SetLogicName(LogicName);
    proto.MutableSBuckets()->SetFreshnessCheckDurationSeconds(FreshnessCheckDuration.Seconds());
}

bool TOptimizerPlannerConstructor::DoDeserializeFromProto(const TProto& proto) {
    if (!proto.HasSBuckets()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse s-buckets optimizer from proto")("proto", proto.DebugString());
        return false;
    }
    LogicName = proto.GetSBuckets().GetLogicName();
    if (proto.GetSBuckets().HasFreshnessCheckDurationSeconds()) {
        FreshnessCheckDuration = TDuration::Seconds(proto.GetSBuckets().GetFreshnessCheckDurationSeconds());
    }
    if (LogicName == "") {
        LogicName = "one_head";
    } else if (LogicName != "one_head" && LogicName != "slices") {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "incorrect s-buckets optimizer logic name")("proto", proto.DebugString());
        return false;
    }
    return true;
}

NKikimr::TConclusionStatus TOptimizerPlannerConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    TString logicNameFromJson;
    if (!jsonInfo["logic_name"].GetString(&logicNameFromJson)) {
        return TConclusionStatus::Fail("no logic_name info in json description");
    }
    if (logicNameFromJson != "one_head" && logicNameFromJson != "slices") {
        return TConclusionStatus::Fail("incorrect logic_type: " + logicNameFromJson + "; have to be one of [one_head, slices]");
    }
    LogicName = logicNameFromJson;
    return TConclusionStatus::Success();
}

bool TOptimizerPlannerConstructor::DoApplyToCurrentObject(IOptimizerPlanner& current) const {
    auto* itemClass = dynamic_cast<TOptimizerPlanner*>(&current);
    if (!itemClass) {
        return false;
    }
    itemClass->ResetLogic(BuildLogic());
    return true;
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
