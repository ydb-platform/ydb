#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap::NCleanDeprecatedSnapshot {

class TCleanDeprecatedSnapshotNormalizer : public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;
    static TString ClassName() {
        return "CleanDeprecatedSnapshot";
    }
    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanDeprecatedSnapshotNormalizer>(ClassName());

    NColumnShard::TBlobGroupSelector DsGroupSelector;

public:
    TCleanDeprecatedSnapshotNormalizer(const TNormalizationController::TInitContext& info)
        : TBase(info)
        , DsGroupSelector(info.GetStorageInfo()) {
    }

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::CleanDeprecatedSnapshot;
    }

    TString GetClassName() const override {
        return ClassName();
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

} //namespace NKikimr::NOlap
