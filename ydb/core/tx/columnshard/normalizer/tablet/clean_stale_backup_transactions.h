#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

class TCleanStaleBackupTransactionsNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::CleanStaleBackupTransactions);
    }

private:
    static inline TFactory::TRegistrator<TCleanStaleBackupTransactionsNormalizer> Registrator =
        TFactory::TRegistrator<TCleanStaleBackupTransactionsNormalizer>(GetClassNameStatic());

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::CleanStaleBackupTransactions;
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

public:
    using TBase::TBase;

    TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap
