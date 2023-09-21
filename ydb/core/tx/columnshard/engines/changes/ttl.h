#pragma once
#include "compaction.h"
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TTTLColumnEngineChanges: public TChangesWithAppend {
private:
    using TPathIdBlobs = THashMap<ui64, THashSet<TUnifiedBlobId>>;
    using TBase = TChangesWithAppend;
    THashMap<TString, TPathIdBlobs> ExportTierBlobs;

    class TPortionForEviction {
    private:
        TPortionInfo PortionInfo;
        TPortionEvictionFeatures Features;
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
            return PortionInfo;
        }
    };

    std::optional<TPortionInfoWithBlobs> UpdateEvictedPortion(TPortionForEviction& info, const THashMap<TBlobRange, TString>& srcBlobs,
        TConstructionContext& context) const;

    std::vector<TPortionForEviction> PortionsToEvict; // {portion, TPortionEvictionFeatures}

protected:
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
public:
    virtual bool NeedConstruction() const override {
        return PortionsToEvict.size();
    }
    virtual THashSet<TPortionAddress> GetTouchedPortions() const override {
        THashSet<TPortionAddress> result = TBase::GetTouchedPortions();
        for (auto&& info : PortionsToEvict) {
            result.emplace(info.GetPortionInfo().GetAddress());
        }
        return result;
    }

    THashMap<ui64, NOlap::TTiering> Tiering;

    ui32 GetPortionsToEvictCount() const {
        return PortionsToEvict.size();
    }

    void AddPortionToEvict(const TPortionInfo& info, TPortionEvictionFeatures&& features) {
        Y_VERIFY(!info.Empty());
        Y_VERIFY(info.IsActive());
        PortionsToEvict.emplace_back(info, std::move(features));
    }

    virtual TString TypeString() const override {
        return "TTL";
    }

    using TBase::TBase;
};

}
