#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

class TRemovedTablesNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::TablesCleaner);
    }

private:
    static inline INormalizerComponent::TFactory::TRegistrator<TRemovedTablesNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TRemovedTablesNormalizer>(GetClassNameStatic());
    class TNormalizerResult;

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::TablesCleaner;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TRemovedTablesNormalizer(const TNormalizationController::TInitContext& context)
        : TBase(context) {
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}   // namespace NKikimr::NOlap
