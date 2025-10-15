#pragma once
#include "abstract/abstract.h"
#include "abstract/remove_portions.h"
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap {

class TCleanupPortionsColumnEngineChanges: public TColumnEngineChanges,
                                           public NColumnShard::TMonitoringObjectsCounter<TCleanupPortionsColumnEngineChanges> {
private:
    using TBase = TColumnEngineChanges;
    THashMap<TString, std::vector<std::shared_ptr<TPortionInfo>>> StoragePortions;
    std::vector<TPortionInfo::TConstPtr> PortionsToDrop;
    TRemovePortionsChange PortionsToRemove;
    THashSet<TInternalPathId> TablesToDrop;

protected:
    virtual void OnDataAccessorsInitialized(const TDataAccessorsInitializationContext& /*context*/) override {
    }

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
    virtual NDataLocks::ELockCategory GetLockCategory() const override {
        return NDataLocks::ELockCategory::Cleanup;
    }
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const override {
        auto portionsDropLock = std::make_shared<NDataLocks::TListPortionsLock>(
            TypeString() + "::PORTIONS_DROP::" + GetTaskIdentifier(), PortionsToDrop, NDataLocks::ELockCategory::Cleanup);
        auto portionsRemoveLock =
            PortionsToRemove.BuildDataLock(TypeString() + "::REMOVE::" + GetTaskIdentifier(), NDataLocks::ELockCategory::Compaction);
        auto tablesLock = std::make_shared<NDataLocks::TListTablesLock>(
            TypeString() + "::TABLES::" + GetTaskIdentifier(), TablesToDrop, NDataLocks::ELockCategory::Tables);
        return NDataLocks::TCompositeLock::Build(TypeString() + "::COMPOSITE::" + GetTaskIdentifier(), {portionsDropLock, portionsRemoveLock, tablesLock});
    }

public:
    TCleanupPortionsColumnEngineChanges(const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(storagesManager, NBlobOperations::EConsumer::CLEANUP_PORTIONS) {
    }

    void AddTableToDrop(const TInternalPathId pathId) {
        TablesToDrop.emplace(pathId);
    }

    const std::vector<TPortionInfo::TConstPtr>& GetPortionsToDrop() const {
        return PortionsToDrop;
    }

    void AddPortionToDrop(const TPortionInfo::TConstPtr& portion) {
        PortionsToDrop.emplace_back(portion);
        PortionsToAccess.emplace_back(portion);
    }

    void AddPortionToRemove(const TPortionInfo::TConstPtr& portion) {
        PortionsToRemove.AddPortion(portion);
        PortionsToAccess.emplace_back(portion);
    }

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

}   // namespace NKikimr::NOlap
