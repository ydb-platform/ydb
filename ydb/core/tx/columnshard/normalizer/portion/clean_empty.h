#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

class TCleanEmptyPortionsNormalizer : public TNormalizationController::INormalizerComponent {

    static TString ClassName() {
        return ToString(ENormalizerSequentialId::EmptyPortionsCleaner);
    }
    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanEmptyPortionsNormalizer>(ClassName());
public:
    TCleanEmptyPortionsNormalizer(const TNormalizationController::TInitContext&)
    {}

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::EmptyPortionsCleaner;
    }

    TString GetClassName() const override {
        return ClassName();
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

} //namespace NKikimr::NOlap
