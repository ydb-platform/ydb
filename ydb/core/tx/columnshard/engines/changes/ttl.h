#pragma once
#include "cleanup.h"
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TTTLColumnEngineChanges: public TCleanupColumnEngineChanges {
private:
    using TPathIdBlobs = THashMap<ui64, THashSet<TUnifiedBlobId>>;
    using TBase = TCleanupColumnEngineChanges;
    THashMap<TString, TPathIdBlobs> ExportTierBlobs;
    ui64 ExportNo = 0;

    bool UpdateEvictedPortion(TPortionInfo& portionInfo,
        TPortionEvictionFeatures& evictFeatures, const THashMap<TBlobRange, TString>& srcBlobs,
        std::vector<TColumnRecord>& evictedRecords, std::vector<TString>& newBlobs, TConstructionContext& context) const;

protected:
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual TConclusion<std::vector<TString>> DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual bool NeedConstruction() const override {
        return PortionsToEvict.size();
    }
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
public:
    std::vector<TColumnRecord> EvictedRecords;
    THashMap<ui64, NOlap::TTiering> Tiering;
    std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>> PortionsToEvict; // {portion, TPortionEvictionFeatures}
    virtual THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GetGroupedBlobRanges() const override;

    virtual void UpdateWritePortionInfo(const ui32 index, const TPortionInfo& info) override {
        PortionsToEvict[index].first = info;
    }
    virtual ui32 GetWritePortionsCount() const override {
        return PortionsToEvict.size();
    }
    virtual const TPortionInfo& GetWritePortionInfo(const ui32 index) const override {
        Y_VERIFY(index < PortionsToEvict.size());
        return PortionsToEvict[index].first;
    }
    virtual bool NeedWritePortion(const ui32 index) const override {
        Y_VERIFY(index < PortionsToEvict.size());
        return PortionsToEvict[index].second.DataChanges;
    }

    virtual TString TypeString() const override {
        return "TTL";
    }
};

}
