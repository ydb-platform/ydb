#pragma once
#include "abstract.h"
#include <util/generic/hash.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TChangesWithAppend: public TColumnEngineChanges {
private:
    THashMap<ui64, NOlap::TTiering> TieringInfo;
protected:
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& /*self*/, TWriteIndexCompleteContext& /*context*/) override {

    }
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    std::vector<TPortionInfo> MakeAppendedPortions(const ui64 pathId,
        const std::shared_ptr<arrow::RecordBatch> batch,
        const ui64 granule,
        const TSnapshot& snapshot,
        std::vector<TString>& blobs, const TGranuleMeta* granuleMeta, TConstructionContext& context) const;

public:
    virtual THashSet<ui64> GetTouchedGranules() const override {
        return {};
    }

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
