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

    class TPortionForEviction {
    private:
        TPortionInfo PortionInfo;
        TPortionEvictionFeatures Features;
        std::optional<TPortionInfoWithBlobs> PortionWithBlobs;
    public:
        TPortionForEviction(const TPortionInfo& portion, TPortionEvictionFeatures&& features)
            : PortionInfo(portion)
            , Features(std::move(features))
        {

        }

        TPortionEvictionFeatures& GetFeatures() {
            return Features;
        }

        const TPortionEvictionFeatures& GetFeatures() const {
            return Features;
        }

        const TPortionInfo& GetPortionInfo() const {
            Y_VERIFY(!PortionWithBlobs);
            return PortionInfo;
        }

        void SetPortionWithBlobs(TPortionInfoWithBlobs&& data) {
            Y_VERIFY(!PortionWithBlobs);
            PortionWithBlobs = std::move(data);
        }

        TPortionInfoWithBlobs& GetPortionWithBlobs() {
            Y_VERIFY(PortionWithBlobs);
            return *PortionWithBlobs;
        }

        const TPortionInfoWithBlobs& GetPortionWithBlobs() const {
            Y_VERIFY(PortionWithBlobs);
            return *PortionWithBlobs;
        }

        const TPortionInfo& GetActualPortionInfo() const {
            return PortionWithBlobs ? PortionWithBlobs->GetPortionInfo() : PortionInfo;
        }
    };

    bool UpdateEvictedPortion(TPortionForEviction& info, const THashMap<TBlobRange, TString>& srcBlobs,
        std::vector<TColumnRecord>& evictedRecords, TConstructionContext& context) const;

    std::vector<TPortionForEviction> PortionsToEvict; // {portion, TPortionEvictionFeatures}

protected:
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
public:
    virtual bool NeedConstruction() const override {
        return PortionsToEvict.size();
    }
    virtual THashSet<TPortionAddress> GetTouchedPortions() const override {
        THashSet<TPortionAddress> result;
        for (auto&& info : PortionsToEvict) {
            result.emplace(info.GetPortionInfo().GetAddress());
        }
        for (auto&& info : PortionsToDrop) {
            result.emplace(info.GetAddress());
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

    virtual ui32 GetWritePortionsCount() const override {
        return PortionsToEvict.size();
    }
    virtual TPortionInfoWithBlobs* GetWritePortionInfo(const ui32 index) override {
        Y_VERIFY(index < PortionsToEvict.size());
        return &PortionsToEvict[index].GetPortionWithBlobs();
    }
    virtual bool NeedWritePortion(const ui32 index) const override {
        Y_VERIFY(index < PortionsToEvict.size());
        return PortionsToEvict[index].GetFeatures().DataChanges;
    }

    virtual TString TypeString() const override {
        return "TTL";
    }
};

}
