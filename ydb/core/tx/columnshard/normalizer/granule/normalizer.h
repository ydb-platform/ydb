#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TGranulesNormalizer: public TNormalizationController::INormalizerComponent {
public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::Granules);
    }
private:
    class TNormalizerResult;

    static const inline INormalizerComponent::TFactory::TRegistrator<TGranulesNormalizer> Registrator = INormalizerComponent::TFactory::TRegistrator<TGranulesNormalizer>(
        GetClassNameStatic());
public:
    TGranulesNormalizer(const TNormalizationController::TInitContext&) {
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::Granules;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
