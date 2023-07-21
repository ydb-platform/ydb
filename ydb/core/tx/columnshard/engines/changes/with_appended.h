#pragma once
#include "abstract.h"
#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TChangesWithAppend: public TColumnEngineChanges {
protected:
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
public:
    std::vector<TPortionInfo> AppendedPortions; // New portions after indexing or compaction
    THashMap<ui64, std::pair<ui64, TMark>> NewGranules; // granule -> {pathId, key}
    ui64 FirstGranuleId = 0;
    virtual ui32 GetWritePortionsCount() const override {
        return AppendedPortions.size();
    }
    virtual const TPortionInfo& GetWritePortionInfo(const ui32 index) const override {
        Y_VERIFY(index < AppendedPortions.size());
        return AppendedPortions[index];
    }
    virtual bool NeedWritePortion(const ui32 /*index*/) const override {
        return true;
    }
    virtual void UpdateWritePortionInfo(const ui32 index, const TPortionInfo& info) override {
        AppendedPortions[index] = info;
    }
};

}
