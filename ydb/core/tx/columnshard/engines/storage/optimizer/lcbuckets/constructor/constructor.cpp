#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/zero_level.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

NKikimr::TConclusion<std::shared_ptr<NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(const TBuildContext& context) const {
    auto counters = std::make_shared<TCounters>();
    std::vector<std::shared_ptr<IPortionsLevel>> levels;
    if (LevelConstructors.size()) {
        std::shared_ptr<IPortionsLevel> nextLevel;
        ui32 idx = LevelConstructors.size();
        for (auto it = LevelConstructors.rbegin(); it != LevelConstructors.rend(); ++it) {
            --idx;
            levels.emplace_back((*it)->BuildLevel(nextLevel, idx, counters->GetLevelCounters(idx)));
            nextLevel = levels.back();
        }
    } else {
        if (context.GetOptimizeForManyTable()) {
            levels.emplace_back(
                std::make_shared<TZeroLevelPortions>(1, nullptr, counters->GetLevelCounters(1), TDuration::Max(), 1 << 20, 10));
            levels.emplace_back(
                std::make_shared<TZeroLevelPortions>(0, levels.back(), counters->GetLevelCounters(0), TDuration::Seconds(180), 1 << 20, 10));
        } else {
            levels.emplace_back(std::make_shared<TZeroLevelPortions>(2, nullptr, counters->GetLevelCounters(2), TDuration::Max(), 1 << 20, 10));
            levels.emplace_back(
                std::make_shared<TZeroLevelPortions>(1, levels.back(), counters->GetLevelCounters(1), TDuration::Max(), 1 << 20, 10));
            levels.emplace_back(
                std::make_shared<TZeroLevelPortions>(0, levels.back(), counters->GetLevelCounters(0), TDuration::Seconds(180), 1 << 20, 10));
        }
    }
    std::reverse(levels.begin(), levels.end());
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), counters, levels);
}

bool TOptimizerPlannerConstructor::DoApplyToCurrentObject(IOptimizerPlanner& current) const {
    auto* itemClass = dynamic_cast<TOptimizerPlanner*>(&current);
    if (!itemClass) {
        return false;
    }
    return true;
}

//N.B. Default constructors, with no configured levels, are considered equal
//despite they may conststuct planners that differ
bool TOptimizerPlannerConstructor::DoIsEqualTo(const IOptimizerPlannerConstructor& item) const {
    const auto* itemClass = dynamic_cast<const TOptimizerPlannerConstructor*>(&item);
    AFL_VERIFY(itemClass);
    if (LevelConstructors.size() != itemClass->LevelConstructors.size()) {
        return false;
    }
    for (ui32 i = 0; i < LevelConstructors.size(); ++i) {
        if (!LevelConstructors[i]->IsEqualTo(*itemClass->LevelConstructors[i].GetObjectPtrVerified())) {
            return false;
        }
    }
    return true;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    *proto.MutableLCBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLCOptimizer();
    for (auto&& i : LevelConstructors) {
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
        LevelConstructors.emplace_back(std::move(lContainer));
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
        LevelConstructors.emplace_back(TLevelConstructorContainer(std::shared_ptr<ILevelConstructor>(level.Release())));
    }
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
