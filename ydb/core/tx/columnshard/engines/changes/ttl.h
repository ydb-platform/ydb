#pragma once
#include "cleanup.h"

namespace NKikimr::NOlap {

class TTTLColumnEngineChanges: public TCleanupColumnEngineChanges {
private:
    using TPathIdBlobs = THashMap<ui64, THashSet<TUnifiedBlobId>>;
    using TBase = TCleanupColumnEngineChanges;
    THashMap<TString, TPathIdBlobs> ExportTierBlobs;
    ui64 ExportNo = 0;
protected:
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
public:
    std::vector<TColumnRecord> EvictedRecords;
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
        return "ttl";
    }
};

}
