#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap::NSyncChunksWithPortions {

class TCleanEmptyPortionsNormalizer : public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;
    static TString ClassName() {
        return "EmptyPortionsCleaner";
    }
    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanEmptyPortionsNormalizer>(ClassName());

    NColumnShard::TBlobGroupSelector DsGroupSelector;

public:
    TCleanEmptyPortionsNormalizer(const TNormalizationController::TInitContext& info)
        : TBase(info)
        , DsGroupSelector(info.GetStorageInfo()) {
    }

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    TString GetClassName() const override {
        return ClassName();
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

} //namespace NKikimr::NOlap
