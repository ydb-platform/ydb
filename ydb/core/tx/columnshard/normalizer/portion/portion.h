#pragma once

#include "normalizer.h"
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap {

class TPortionsNormalizer : public TPortionsNormalizerBase {
public:
    class TNormalizerResult;

public:
    TPortionsNormalizer(TTabletStorageInfo* info)
        : TPortionsNormalizerBase(info)
    {}

    virtual const TString& GetName() const override {
        const static TString name = "TPortionsNormalizer";
        return name;
    }

    virtual INormalizerTask::TPtr BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const override;
    virtual TConclusion<bool> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool CheckPortion(const TPortionInfo& portionInfo) const override;

private:
    THashSet<TPortionAddress> KnownPortions;

};

}
