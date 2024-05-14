#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TRemovedTablesNormalizer: public TNormalizationController::INormalizerComponent {
    static inline INormalizerComponent::TFactory::TRegistrator<TRemovedTablesNormalizer> Registrator = INormalizerComponent::TFactory::TRegistrator<TRemovedTablesNormalizer>(ENormalizerSequentialId::TablesCleaner);
    class TNormalizerResult;
public:
    TRemovedTablesNormalizer(const TNormalizationController::TInitContext&)
    {}

    virtual ENormalizerSequentialId GetType() const override {
        return ENormalizerSequentialId::TablesCleaner;
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
