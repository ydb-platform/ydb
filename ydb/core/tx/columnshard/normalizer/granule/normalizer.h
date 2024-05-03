#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TGranulesNormalizer: public TNormalizationController::INormalizerComponent {
    class TNormalizerResult;

    static inline INormalizerComponent::TFactory::TRegistrator<TGranulesNormalizer> Registrator = INormalizerComponent::TFactory::TRegistrator<TGranulesNormalizer>(ENormalizerSequentialId::Granules);
public:
    TGranulesNormalizer(const TNormalizationController::TInitContext&) {
    }

    virtual ENormalizerSequentialId GetType() const override {
        return ENormalizerSequentialId::Granules;
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
