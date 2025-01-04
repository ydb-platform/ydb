#pragma once

#include "normalizer.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NColumnShard {
class TTablesManager;
}

namespace NKikimr::NOlap {

class TCleanPortionsNormalizer: public TPortionsNormalizerBase {
public:
    static TString GetClassNameStatic() {
        return "PortionsCleaner";
    }

private:
    static inline TFactory::TRegistrator<TCleanPortionsNormalizer> Registrator =
        TFactory::TRegistrator<TCleanPortionsNormalizer>(GetClassNameStatic());

public:
    class TNormalizerResult;
    class TTask;

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TCleanPortionsNormalizer(const TNormalizationController::TInitContext& info)
        : TPortionsNormalizerBase(info) {
    }

    virtual std::set<ui32> GetColumnsFilter(const ISnapshotSchema::TPtr& /*schema*/) const override {
        return {};
    }

    virtual INormalizerTask::TPtr BuildTask(
        std::vector<TPortionDataAccessor>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const override;
    virtual TConclusion<bool> DoInitImpl(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool CheckPortion(const NColumnShard::TTablesManager& tablesManager, const TPortionDataAccessor& portionInfo) const override;
};

}   // namespace NKikimr::NOlap
