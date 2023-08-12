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
    std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>> PortionsToEvict; // {portion, TPortionEvictionFeatures}

protected:
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual TConclusion<std::vector<TString>> DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
public:
    virtual bool NeedConstruction() const override {
        return PortionsToEvict.size();
    }
    virtual THashSet<ui64> GetTouchedGranules() const override {
        auto result = TBase::GetTouchedGranules();
        for (const auto& [portionInfo, _] : PortionsToEvict) {
            result.emplace(portionInfo.GetGranule());
        }
        return result;
    }

    std::vector<TColumnRecord> EvictedRecords;
    THashMap<ui64, NOlap::TTiering> Tiering;
    virtual THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GetGroupedBlobRanges() const override;

    void AddPortionToEvict(const TPortionInfo& info, TPortionEvictionFeatures&& features) {
        Y_VERIFY(!info.Empty());
        Y_VERIFY(info.IsActive());
        PortionsToEvict.emplace_back(info, std::move(features));
    }

    ui32 GetPortionsToEvictCount() const {
        return PortionsToEvict.size();
    }

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
