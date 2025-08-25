#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/transparent.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level/zero_level.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level/common_level.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TConclusion<std::shared_ptr<IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(const TBuildContext& context) const {
    auto counters = std::make_shared<TCounters>();
    auto portionsInfo =  std::make_shared<TSimplePortionsGroupInfo>();
    const TString defaultSelectorName = "default";
    std::vector<std::shared_ptr<IPortionsSelector>> selectors;
    {
        std::set<TString> selectorNames;
        for (auto&& i : SelectorConstructors) {
            AFL_VERIFY(selectorNames.emplace(i->GetName()).second);
            selectors.emplace_back(i->BuildSelector());
        }
        if (selectors.empty()) {
            selectors = { std::make_shared<TTransparentPortionsSelector>(defaultSelectorName) };
        }
    }
    std::vector<std::shared_ptr<IPortionsLevel>> levels;
    if (LevelConstructors.size()) {
        std::shared_ptr<IPortionsLevel> nextLevel;
        ui32 idx = LevelConstructors.size();
        for (auto it = LevelConstructors.rbegin(); it != LevelConstructors.rend(); ++it) {
            --idx;
            levels.emplace_back((*it)->BuildLevel(nextLevel, idx, portionsInfo, counters->GetLevelCounters(idx), selectors));
            nextLevel = levels.back();
        }
    } else {
        switch(context.GetDefaultStrategy()) {
            case EOptimizerStrategy::Default:
                levels.emplace_back(std::make_shared<TOneLayerPortions>(
                    5, 1.0,  8 * (1ull << 20),
                    nullptr, portionsInfo, counters->GetLevelCounters(5),
                    1ull << 40,
                    selectors, defaultSelectorName
                ));

                levels.emplace_back(std::make_shared<TOneLayerPortions>(
                    4, 0.0, 4 * (1ull << 20),
                    levels.back(), portionsInfo, counters->GetLevelCounters(4),
                    16 * (1ull << 30),
                    selectors, defaultSelectorName
                ));

                levels.emplace_back(std::make_shared<TOneLayerPortions>(
                    3, 0.0, 2 * (1ull << 20),
                    levels.back(), portionsInfo, counters->GetLevelCounters(3),
                    1ull << 30,
                    selectors, defaultSelectorName
                ));

                levels.emplace_back(std::make_shared<TOneLayerPortions>(
                    2, 0.0, 1 * (1 << 20),
                    levels.back(), portionsInfo, counters->GetLevelCounters(2),
                    128 * (1ull << 20),
                    selectors, defaultSelectorName
                ));

                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    1, levels.back(), counters->GetLevelCounters(1), 
                    std::make_shared<TLimitsOverloadChecker>(1ull << 20, 16 * (1ull << 30)), 
                    TDuration::Max(), 2 * (1ull << 20), 1,
                    selectors, defaultSelectorName
                ));

                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    0, levels.back(), counters->GetLevelCounters(0), 
                    std::make_shared<TLimitsOverloadChecker>(1ull << 20, 8 * (1ull << 30)), 
                    TDuration::Max(), 1ull << 20, 1,
                    selectors, defaultSelectorName
                ));
               break;

            case EOptimizerStrategy::Logs:
                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    1, nullptr, counters->GetLevelCounters(1),
                    std::make_shared<TNoOverloadChecker>(),
                    TDuration::Max(), 8 << 20, 1,
                    selectors, defaultSelectorName
                ));
                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    0, levels.back(), counters->GetLevelCounters(0),
                    std::make_shared<TLimitsOverloadChecker>(1'000'000, 8 * (1ull << 30)),
                    TDuration::Max(), 4 << 20, 1, 
                    selectors, defaultSelectorName
                ));
                break;

            case EOptimizerStrategy::LogsInStore:
                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    2, nullptr, counters->GetLevelCounters(2),
                    std::make_shared<TNoOverloadChecker>(), 
                    TDuration::Max(), 8 * (1ull << 20), 1,
                    selectors, defaultSelectorName
                ));
                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    1, levels.back(), counters->GetLevelCounters(1),
                    std::make_shared<TLimitsOverloadChecker>(1'000'000, 8 * (1ull << 30)),
                    TDuration::Max(), 4 * (1ull << 20), 1,
                    selectors, defaultSelectorName
                ));
                levels.emplace_back(std::make_shared<TZeroLevelPortions>(
                    0, levels.back(), counters->GetLevelCounters(0),
                    std::make_shared<TLimitsOverloadChecker>(1'000'000, 8 * (1ull << 30)),
                    TDuration::Seconds(180), 2 * (1ull << 20), 1,
                    selectors, defaultSelectorName
                ));
                break;
        }
    }
    std::reverse(levels.begin(), levels.end());
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), counters, portionsInfo,
        std::move(levels), std::move(selectors), GetNodePortionsCountLimit());
}

bool TOptimizerPlannerConstructor::DoApplyToCurrentObject(IOptimizerPlanner& /*current*/) const {
    return false;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    *proto.MutableLCBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLCOptimizer();
    for (auto&& i : LevelConstructors) {
        *proto.MutableLCBuckets()->AddLevels() = i.SerializeToProto();
    }
    for (auto&& i : SelectorConstructors) {
        *proto.MutableLCBuckets()->AddSelectors() = i.SerializeToProto();
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
    for (auto&& i : proto.GetLCBuckets().GetSelectors()) {
        TSelectorConstructorContainer container;
        if (!container.DeserializeFromProto(i)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse lc-bucket selector")("proto", i.DebugString());
            return false;
        }
        SelectorConstructors.emplace_back(std::move(container));
    }
    return true;
}

TConclusionStatus TOptimizerPlannerConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    std::set<TString> selectorNames;
    if (jsonInfo.Has("selectors")) {
        if (!jsonInfo["selectors"].IsArray()) {
            return TConclusionStatus::Fail("selectors have to been array in json description");
        }
        auto& arr = jsonInfo["selectors"].GetArray();
        if (!arr.size()) {
            return TConclusionStatus::Fail("no objects in json array 'selectors'");
        }
        for (auto&& i : arr) {
            const auto className = i["class_name"].GetStringRobust();
            auto selector = ISelectorConstructor::TFactory::MakeHolder(className);
            if (!selector) {
                return TConclusionStatus::Fail("incorrect portions selector class_name: " + className);
            }
            if (!selector->DeserializeFromJson(i)) {
                return TConclusionStatus::Fail("cannot parse portions selector: " + i.GetStringRobust());
            }
            SelectorConstructors.emplace_back(TSelectorConstructorContainer(std::shared_ptr<ISelectorConstructor>(selector.Release())));
            if (!selectorNames.emplace(SelectorConstructors.back()->GetName()).second) {
                return TConclusionStatus::Fail("selector name duplication: '" + SelectorConstructors.back()->GetName() + "'");
            }
        }
    }
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
        auto parseConclusion = level->DeserializeFromJson(i);
        if (parseConclusion.IsFail()) {
            return TConclusionStatus::Fail("cannot parse level: " + i.GetStringRobust() + "; " + parseConclusion.GetErrorMessage());
        }
        LevelConstructors.emplace_back(TLevelConstructorContainer(std::shared_ptr<ILevelConstructor>(level.Release())));
        if (selectorNames.empty()) {
            if (LevelConstructors.back()->GetDefaultSelectorName() != "default") {
                return TConclusionStatus::Fail("incorrect default selector name for level: '" + LevelConstructors.back()->GetDefaultSelectorName() + "'");
            }
        } else {
            if (!selectorNames.contains(LevelConstructors.back()->GetDefaultSelectorName())) {
                return TConclusionStatus::Fail("unknown default selector name for level: '" + LevelConstructors.back()->GetDefaultSelectorName() + "'");
            }
        }
    }
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
