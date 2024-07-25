#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TDeleteUnsupportedSpecialColumnsNormalier: public TNormalizationController::INormalizerComponent {
    using TThisClass = TDeleteUnsupportedSpecialColumnsNormalier;
    static constexpr auto TypeId = ENormalizerSequentialId::DeleteUnsupportedSpecialColumns;
    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TThisClass>(TypeId);
public:
    TDeleteUnsupportedSpecialColumnsNormalier(const TNormalizationController::TInitContext&)
    {}

    virtual ENormalizerSequentialId GetType() const override {
        return TypeId;
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
