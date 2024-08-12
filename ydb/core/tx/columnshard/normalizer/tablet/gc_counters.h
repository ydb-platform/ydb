#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TGCCountersNormalizer: public TNormalizationController::INormalizerComponent {
public:
    static TString GetClassNameStatic() {
        return "GCCountersNormalizer";
    }
private:
    class TNormalizerResult;

    static const inline INormalizerComponent::TFactory::TRegistrator<TGCCountersNormalizer> Registrator = 
        INormalizerComponent::TFactory::TRegistrator<TGCCountersNormalizer>(GetClassNameStatic());
public:
    TGCCountersNormalizer(const TNormalizationController::TInitContext&) {
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
