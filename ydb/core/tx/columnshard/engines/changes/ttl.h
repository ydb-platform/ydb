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

        TPortionInfo& MutablePortionInfo() {
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
    virtual ui64 DoCalcMemoryForUsage() const override {
        ui64 result = 0;
        for (auto& p : PortionsToEvict) {
            result += 2 * p.GetPortionInfo().GetBlobBytes();
        }
        return result;
    }
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
        Y_ABORT_UNLESS(!info.Empty());
        Y_ABORT_UNLESS(!info.HasRemoveSnapshot());
        PortionsToEvict.emplace_back(info, std::move(features));
    }

    static TString StaticTypeName() {
        return "CS::TTL";
    }

    virtual TString TypeString() const override {
        return StaticTypeName();
    }

    TTTLColumnEngineChanges(const TSplitSettings& splitSettings, const TSaverContext& saverContext)
        : TBase(splitSettings, saverContext, StaticTypeName()) {

    }

};

}
