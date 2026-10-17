#pragma once

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

class TCleanOrphanedOperationsNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::CleanOrphanedOperations);
    }

private:
    class TChanges;

    static const inline INormalizerComponent::TFactory::TRegistrator<TCleanOrphanedOperationsNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TCleanOrphanedOperationsNormalizer>(GetClassNameStatic());

    const NColumnShard::TBlobGroupSelector DsGroupSelector;

public:
    TCleanOrphanedOperationsNormalizer(const TNormalizationController::TInitContext& context)
        : TBase(context)
        , DsGroupSelector(context.GetStorageInfo())
    {
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::CleanOrphanedOperations;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}   // namespace NKikimr::NOlap
