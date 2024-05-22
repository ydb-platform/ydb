#pragma once

#include "normalizer.h"
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap {

class TPortionsNormalizer : public TPortionsNormalizerBase {
    static inline TFactory::TRegistrator<TPortionsNormalizer> Registrator = TFactory::TRegistrator<TPortionsNormalizer>(ENormalizerSequentialId::PortionsMetadata);
public:
    class TNormalizerResult;
    class TTask;

public:
    TPortionsNormalizer(const TNormalizationController::TInitContext& info)
        : TPortionsNormalizerBase(info)
    {}

    virtual ENormalizerSequentialId GetType() const override {
        return ENormalizerSequentialId::PortionsMetadata;
    }

    virtual INormalizerTask::TPtr BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const override;
    virtual TConclusion<bool> DoInitImpl(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool CheckPortion(const NColumnShard::TTablesManager& tablesManager, const TPortionInfo& portionInfo) const override;

private:
    THashSet<TPortionAddress> KnownPortions;

};

}
