#pragma once

#include "normalizer.h"
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap {

class TCleanPortionsNormalizer : public TPortionsNormalizerBase {
public:
    class TNormalizerResult;
    class TTask;

public:
    TCleanPortionsNormalizer(TTabletStorageInfo* info)
        : TPortionsNormalizerBase(info)
    {}

    virtual const TString& GetName() const override {
        const static TString name = "TCleanPortionsNormalizer";
        return name;
    }

    virtual std::set<ui32> GetColumnsFilter(const ISnapshotSchema::TPtr& /*schema*/) const override {
        return {};
    }

    virtual INormalizerTask::TPtr BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const override;
    virtual TConclusion<bool> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool CheckPortion(const NColumnShard::TTablesManager& tablesManager, const TPortionInfo& portionInfo) const override;
};

}
