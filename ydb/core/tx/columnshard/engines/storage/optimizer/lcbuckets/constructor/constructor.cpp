#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

NKikimr::TConclusion<std::shared_ptr<NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(const TBuildContext& context) const {
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Levels);
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
    if (Levels.size() != itemClass->Levels.size()) {
        return false;
    }
    for (ui32 i = 0; i < Levels.size(); ++i) {
        if (!Levels[i]->IsEqualTo(*itemClass->Levels[i].GetObjectPtrVerified())) {
            return false;
        }
    }
    return true;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    *proto.MutableLCBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLCOptimizer();
    for (auto&& i : Levels) {
        *proto.MutableLCBuckets()->AddLevels() = i.SerializeToProto();
    }
}

bool TOptimizerPlannerConstructor::DoDeserializeFromProto(const TProto& proto) {
    if (!proto.HasLCBuckets()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse lc-buckets optimizer from proto")("proto", proto.DebugString());
        return false;
    }
    for (auto&& i : proto.GetLCBuckets().GetLevels()) {
        TLevelConstructorContainer lContainer;
        if (!lContainer.DeserializeFromProto(i)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse lc-bucket level")("proto", i.DebugString());
            return false;
        }
        Levels.emplace_back(std::move(lContainer));
    }
    return true;
}

NKikimr::TConclusionStatus TOptimizerPlannerConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("levels")) {
        return TConclusionStatus::Fail("no levels description");
    }
    if (!jsonInfo["levels"].IsArray()) {
        return TConclusionStatus::Fail("levels have to been array in json description");
    }
    auto& arr = jsonInfo["levels"].GetArray();
    if (!arr.size()) {
        return TConclusionStatus::Fail("no objects in json array 'levels'");
    }
    for (auto&& i : arr) {
        const auto className = i["class_name"].GetStringRobust();
        auto level = ILevelConstructor::TFactory::MakeHolder(className);
        if (!level) {
            return TConclusionStatus::Fail("incorrect level class_name: " + className);
        }
        if (!level->DeserializeFromJson(i)) {
            return TConclusionStatus::Fail("cannot parse level: " + i.GetStringRobust());
        }
        Levels.emplace_back(TLevelConstructorContainer(std::shared_ptr<ILevelConstructor>(level.Release())));
    }
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
