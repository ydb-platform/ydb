#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TBrokenTxsNormalizer: public TNormalizationController::INormalizerComponent {
public:
    static TString GetClassNameStatic() {
        return "BrokenTxsNormalizer";
    }
private:
    class TNormalizerResult;

    static const inline INormalizerComponent::TFactory::TRegistrator<TBrokenTxsNormalizer> Registrator = 
        INormalizerComponent::TFactory::TRegistrator<TBrokenTxsNormalizer>(GetClassNameStatic());

public:
    TBrokenTxsNormalizer(const TNormalizationController::TInitContext&) {
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return {};
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
