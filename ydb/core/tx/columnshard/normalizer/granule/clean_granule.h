#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TCleanGranuleIdNormalizer: public TNormalizationController::INormalizerComponent {
    class TNormalizerResult;

    static inline INormalizerComponent::TFactory::TRegistrator<TCleanGranuleIdNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TCleanGranuleIdNormalizer>(ENormalizerSequentialId::CleanGranuleId);
public:
    TCleanGranuleIdNormalizer(const TNormalizationController::TInitContext&) {
    }

    virtual ENormalizerSequentialId GetType() const override {
        return ENormalizerSequentialId::CleanGranuleId;
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
