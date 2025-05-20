#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TConclusion<std::shared_ptr<IOptimizerPlanner>> TOptimizerPlannerConstructor::DoBuildPlanner(const TBuildContext& context) const {
    return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Levels, Selectors);
}

bool TOptimizerPlannerConstructor::DoApplyToCurrentObject(IOptimizerPlanner& /*current*/) const {
    return false;
}

void TOptimizerPlannerConstructor::DoSerializeToProto(TProto& proto) const {
    *proto.MutableLCBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLCOptimizer();
    for (auto&& i : Levels) {
        *proto.MutableLCBuckets()->AddLevels() = i.SerializeToProto();
    }
    for (auto&& i : Selectors) {
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
        Levels.emplace_back(std::move(lContainer));
    }
    for (auto&& i : proto.GetLCBuckets().GetSelectors()) {
        TSelectorConstructorContainer container;
        if (!container.DeserializeFromProto(i)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse lc-bucket selector")("proto", i.DebugString());
            return false;
        }
        Selectors.emplace_back(std::move(container));
    }
    return true;
}

NKikimr::TConclusionStatus TOptimizerPlannerConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
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
            Selectors.emplace_back(TSelectorConstructorContainer(std::shared_ptr<ISelectorConstructor>(selector.Release())));
            if (!selectorNames.emplace(Selectors.back()->GetName()).second) {
                return TConclusionStatus::Fail("selector name duplication: '" + Selectors.back()->GetName() + "'");
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
        Levels.emplace_back(TLevelConstructorContainer(std::shared_ptr<ILevelConstructor>(level.Release())));
        if (selectorNames.empty()) {
            if (Levels.back()->GetDefaultSelectorName() != "default") {
                return TConclusionStatus::Fail("incorrect default selector name for level: '" + Levels.back()->GetDefaultSelectorName() + "'");
            }
        } else {
            if (!selectorNames.contains(Levels.back()->GetDefaultSelectorName())) {
                return TConclusionStatus::Fail("unknown default selector name for level: '" + Levels.back()->GetDefaultSelectorName() + "'");
            }
        }
    }
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
