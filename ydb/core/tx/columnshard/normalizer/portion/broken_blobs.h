#pragma once

#include "normalizer.h"
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap::NNormalizer::NBrokenBlobs {

class TNormalizer : public TPortionsNormalizerBase {
public:
    static TString GetClassNameStatic() {
        return "BROKEN_BLOBS";
    }

private:
    static inline TFactory::TRegistrator<TNormalizer> Registrator = TFactory::TRegistrator<TNormalizer>(GetClassNameStatic());
public:

    class TNormalizerResult;
    class TTask;

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return {};
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TNormalizer(const TNormalizationController::TInitContext& info)
        : TPortionsNormalizerBase(info)
    {}

    virtual std::set<ui32> GetColumnsFilter(const ISnapshotSchema::TPtr& /*schema*/) const override {
        return {};
    }

    virtual INormalizerTask::TPtr BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const override;
    virtual TConclusion<bool> DoInitImpl(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool CheckPortion(const NColumnShard::TTablesManager& tablesManager, const TPortionInfo& portionInfo) const override;
};

}
