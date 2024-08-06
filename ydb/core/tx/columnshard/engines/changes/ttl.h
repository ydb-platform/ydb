#pragma once
#include "compaction.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>

#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TTTLColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;

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

    std::optional<TWritePortionInfoWithBlobsResult> UpdateEvictedPortion(TPortionForEviction& info, NBlobOperations::NRead::TCompositeReadBlobs& srcBlobs,
        TConstructionContext& context) const;

    std::vector<TPortionForEviction> PortionsToEvict;
    const NActualizer::TRWAddress RWAddress;
protected:
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
    virtual ui64 DoCalcMemoryForUsage() const override {
        auto predictor = BuildMemoryPredictor();
        ui64 result = 0;
        for (auto& p : PortionsToEvict) {
            result = predictor->AddPortion(p.GetPortionInfo());
        }
        return result;
    }
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const override {
        const auto pred = [](const TPortionForEviction& p) {
            return p.GetPortionInfo().GetAddress();
        };
        return std::make_shared<NDataLocks::TListPortionsLock>(TypeString() + "::" + RWAddress.DebugString() + "::" + GetTaskIdentifier(), PortionsToEvict, pred);
    }
public:
    class TMemoryPredictorSimplePolicy: public IMemoryPredictor {
    private:
        ui64 SumBlobsMemory = 0;
        ui64 MaxRawMemory = 0;
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) override {
            if (MaxRawMemory < portionInfo.GetTotalRawBytes()) {
                MaxRawMemory = portionInfo.GetTotalRawBytes();
            }
            SumBlobsMemory += portionInfo.GetTotalBlobBytes();
            return SumBlobsMemory + MaxRawMemory;
        }
    };

    const NActualizer::TRWAddress& GetRWAddress() const {
        return RWAddress;
    }

    static std::shared_ptr<IMemoryPredictor> BuildMemoryPredictor() {
        return std::make_shared<TMemoryPredictorSimplePolicy>();
    }

    virtual bool NeedConstruction() const override {
        return PortionsToEvict.size();
    }
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

    TTTLColumnEngineChanges(const NActualizer::TRWAddress& address, const TSaverContext& saverContext)
        : TBase(saverContext, NBlobOperations::EConsumer::TTL)
        , RWAddress(address)
    {

    }

};

}
