#pragma once
#include "abstract/abstract.h"

namespace NKikimr::NOlap {

class TCleanupPortionsColumnEngineChanges: public TColumnEngineChanges {
private:
    using TBase = TColumnEngineChanges;
    THashMap<TString, THashSet<NOlap::TEvictedBlob>> BlobsToForget;
    THashMap<TString, std::vector<std::shared_ptr<TPortionInfo>>> StoragePortions;
protected:
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override;

    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& /*context*/) override {
    }
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& /*context*/) noexcept override {
        return TConclusionStatus::Success();
    }
    virtual bool NeedConstruction() const override {
        return false;
    }
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
    virtual ui64 DoCalcMemoryForUsage() const override {
        return 0;
    }
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const override {
        return std::make_shared<NDataLocks::TListPortionsLock>(TypeString() + "::" + GetTaskIdentifier(), PortionsToDrop);
    }

public:
    TCleanupPortionsColumnEngineChanges(const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(storagesManager, NBlobOperations::EConsumer::CLEANUP_PORTIONS) {

    }

    std::vector<TPortionInfo> PortionsToDrop;

    virtual ui32 GetWritePortionsCount() const override {
        return 0;
    }
    virtual TWritePortionInfoWithBlobsResult* GetWritePortionInfo(const ui32 /*index*/) override {
        return nullptr;
    }
    virtual bool NeedWritePortion(const ui32 /*index*/) const override {
        return false;
    }

    static TString StaticTypeName() {
        return "CS::CLEANUP::PORTIONS";
    }

    virtual TString TypeString() const override {
        return StaticTypeName();
    }
};

}
