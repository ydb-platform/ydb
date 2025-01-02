#pragma once
#include "settings.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/locks/composite.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

#include <compare>

namespace NKikimr::NTabletFlatExecutor {
class TTransactionContext;
}

namespace NKikimr::NColumnShard {
class TBlobManagerDb;
class TColumnShard;
}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class TVersionedIndex;

class TPortionEvictionFeatures {
private:
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, CurrentScheme);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, TargetScheme);
    std::optional<TString> TargetTierName;
    const TString CurrentTierName;
    std::optional<NActualizer::TRWAddress> RWAddress;

public:
    TPortionEvictionFeatures(const std::shared_ptr<ISnapshotSchema>& currentScheme, const std::shared_ptr<ISnapshotSchema>& targetScheme,
        const TString& currentTierName)
        : CurrentScheme(currentScheme)
        , TargetScheme(targetScheme)
        , CurrentTierName(currentTierName) {
        AFL_VERIFY(CurrentTierName);
    }

    const TString& GetTargetTierName() const {
        AFL_VERIFY(TargetTierName);
        return *TargetTierName;
    }

    void SetTargetTierName(const TString& value) {
        AFL_VERIFY(!TargetTierName);
        TargetTierName = value;
    }

    void OnSkipPortionWithProcessMemory(const NColumnShard::TEngineLogsCounters& counters, const TDuration dWait) const {
        if (TargetTierName == NTiering::NCommon::DeleteTierName) {
            counters.OnSkipDeleteWithProcessMemory(dWait);
        } else {
            counters.OnSkipEvictionWithProcessMemory(dWait);
        }
    }

    void OnSkipPortionWithTxLimit(const NColumnShard::TEngineLogsCounters& counters, const TDuration dWait) const {
        if (TargetTierName == NTiering::NCommon::DeleteTierName) {
            counters.OnSkipDeleteWithTxLimit(dWait);
        } else {
            counters.OnSkipEvictionWithTxLimit(dWait);
        }
    }

    NActualizer::TRWAddress GetRWAddress() {
        if (!RWAddress) {
            AFL_VERIFY(TargetTierName);
            RWAddress = NActualizer::TRWAddress(CurrentScheme->GetIndexInfo().GetUsedStorageIds(CurrentTierName),
                TargetScheme->GetIndexInfo().GetUsedStorageIds(*TargetTierName));
        }
        return *RWAddress;
    }

    bool NeedRewrite() const {
        if (TargetTierName == NTiering::NCommon::DeleteTierName) {
            return false;
        }
        if (CurrentTierName != TargetTierName) {
            return true;
        }
        if (CurrentScheme->GetVersion() != TargetScheme->GetVersion()) {
            return true;
        }
        AFL_VERIFY(false);
        return false;
    }
};

class TFinalizationContext: TNonCopyable {
private:
    ui64* LastGranuleId;
    ui64* LastPortionId;
    const TSnapshot Snapshot;

public:
    TFinalizationContext(ui64& lastGranuleId, ui64& lastPortionId, const TSnapshot& snapshot)
        : LastGranuleId(&lastGranuleId)
        , LastPortionId(&lastPortionId)
        , Snapshot(snapshot) {
    }

    ui64 NextGranuleId() {
        return ++(*LastGranuleId);
    }
    ui64 NextPortionId() {
        return ++(*LastPortionId);
    }
    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }
};

class TWriteIndexContext: TNonCopyable {
public:
    NTable::TDatabase* DB;
    IDbWrapper& DBWrapper;
    TColumnEngineForLogs& EngineLogs;
    const TSnapshot Snapshot;
    TWriteIndexContext(NTable::TDatabase* db, IDbWrapper& dbWrapper, TColumnEngineForLogs& engineLogs, const TSnapshot& snapshot);
};

class TChangesFinishContext {
public:
    const bool FinishedSuccessfully = true;
    const TString ErrorMessage;
    TChangesFinishContext(const TString& errorMessage)
        : FinishedSuccessfully(false)
        , ErrorMessage(errorMessage) {
    }

    TChangesFinishContext() = default;
};

class TWriteIndexCompleteContext: TNonCopyable, public TChangesFinishContext {
private:
    using TBase = TChangesFinishContext;

public:
    const TActorContext& ActorContext;
    const ui32 BlobsWritten;
    const ui64 BytesWritten;
    const TDuration Duration;
    TColumnEngineForLogs& EngineLogs;
    const TSnapshot Snapshot;
    TWriteIndexCompleteContext(const TActorContext& actorContext, const ui32 blobsWritten, const ui64 bytesWritten, const TDuration d,
        TColumnEngineForLogs& engineLogs, const TSnapshot& snapshot)
        : ActorContext(actorContext)
        , BlobsWritten(blobsWritten)
        , BytesWritten(bytesWritten)
        , Duration(d)
        , EngineLogs(engineLogs)
        , Snapshot(snapshot) {
    }
};

class TConstructionContext: TNonCopyable {
public:
    const TVersionedIndex& SchemaVersions;
    const NColumnShard::TIndexationCounters Counters;
    const NOlap::TSnapshot LastCommittedTx;

    TConstructionContext(
        const TVersionedIndex& schemaVersions, const NColumnShard::TIndexationCounters& counters, const NOlap::TSnapshot& lastCommittedTx)
        : SchemaVersions(schemaVersions)
        , Counters(counters)
        , LastCommittedTx(lastCommittedTx) {
    }
};

class TGranuleMeta;

class TDataAccessorsInitializationContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TVersionedIndex>, VersionedIndex);

public:
    TDataAccessorsInitializationContext(const std::shared_ptr<TVersionedIndex>& versionedIndex)
        : VersionedIndex(versionedIndex) {
        AFL_VERIFY(VersionedIndex);
    }
};

class IChangeAction {
private:
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const = 0;
    virtual void DoApplyOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) = 0;
    virtual void DoApplyOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) = 0;

public:
    IChangeAction() = default;

    std::shared_ptr<NDataLocks::ILock> BuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const {
        return DoBuildDataLock(id, lockCategory);
    }

    void ApplyOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
        return DoApplyOnExecute(self, context);
    }

    void ApplyOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
        return DoApplyOnComplete(self, context);
    }
};

class TRemovePortionsChange: public IChangeAction {
private:
    THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>> Portions;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const override {
        THashSet<TPortionAddress> portions;
        for (auto&& i : Portions) {
            AFL_VERIFY(portions.emplace(i.first).second);
        }
        return std::make_shared<NDataLocks::TListPortionsLock>(id, portions, lockCategory);
    }

    virtual void DoApplyOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override {
        THashSet<ui64> usedPortionIds;
        auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();
        for (auto&& [_, i] : Portions) {
            Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
            AFL_VERIFY(usedPortionIds.emplace(i->GetPortionId()).second)("portion_info", i->DebugString(true));
            const auto pred = [&](TPortionInfo& portionCopy) {
                portionCopy.SetRemoveSnapshot(context.Snapshot);
            };
            context.EngineLogs.GetGranuleVerified(i->GetPathId())
                .ModifyPortionOnExecute(
                    context.DBWrapper, GetPortionDataAccessor(i->GetPortionId()), pred, schemaPtr->GetIndexInfo().GetPKFirstColumnId());
        }
    }

    virtual void DoApplyOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override {
        if (self) {
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, Portions.size());

            for (auto& [_, portionInfo] : Portions) {
                self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BLOBS_DEACTIVATED, portionInfo->GetBlobIdsCount());
                for (auto& blobId : portionInfo->GetBlobIds()) {
                    self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
                }
                self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo->GetTotalRawBytes());
            }
        }

        auto g = context.EngineLogs.GranulesStorage->GetStats()->StartPackModification();
        for (auto&& [_, i] : Portions) {
            Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
            const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
                portion->SetRemoveSnapshot(context.Snapshot);
            };
            context.EngineLogs.ModifyPortionOnComplete(i, pred);
            context.EngineLogs.AddCleanupPortion(i);
        }
    }

public:
    THashSet<ui64> GetPortionIds() const {
        THashSet<ui64> result;
        for (auto&& i : Portions) {
            result.emplace(i.first.GetPortionId());
        }
        return result;
    }

    const THashMap<TPortionAddress, TPortionInfo::TConstPtr>& GetPortionsToRemove() const {
        return PortionsToRemove;
    }

    ui32 GetSize() const {
        return Portions.size();
    }

    bool HasPortions() const {
        return Portions.size();
    }

    bool AddPortion(const TPortionInfo::TConstPtr& info) {
        AFL_VERIFY(!info->HasRemoveSnapshot());
        return Portions.emplace(info->GetAddress(), info).second;
    }
};

class TMovePortionsChange: public IChangeAction {
private:
    THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>> Portions;
    std::optional<ui64> TargetCompactionLevel;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const override {
        THashSet<TPortionAddress> portions;
        for (auto&& i : Portions) {
            AFL_VERIFY(portions.emplace(i.first).second);
        }
        return std::make_shared<NDataLocks::TListPortionsLock>(id, portions, lockCategory);
    }

    virtual void DoApplyOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override {
        for (auto&& [_, i] : Portions) {
            const auto pred = [&](TPortionInfo& portionCopy) {
                portionCopy.MutableMeta().ResetCompactionLevel(TargetCompactionLevel.value_or(0));
            };
            context.EngineLogs.GetGranuleVerified(i->GetPathId())
                .ModifyPortionOnExecute(
                    context.DBWrapper, GetPortionDataAccessor(i->GetPortionId()), pred, schemaPtr->GetIndexInfo().GetPKFirstColumnId());
        }
    }

    virtual void DoApplyOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override {
        if (!Portions.size()) {
            return;
        }
        THashMap<ui32, TSimplePortionsGroupInfo> portionGroups;
        for (auto&& [_, i] : PortionsToMove) {
            portionGroups[i->GetMeta().GetCompactionLevel()].AddPortion(i);
        }
        NChanges::TGeneralCompactionCounters::OnMovePortionsByLevel(portionGroups, TargetCompactionLevel.value_or(0));
        for (auto&& [_, i] : PortionsToMove) {
            const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
                portion->MutableMeta().ResetCompactionLevel(TargetCompactionLevel.value_or(0));
            };
            context.EngineLogs.ModifyPortionOnComplete(i, pred);
        }
    }

public:
    const THashMap<TPortionAddress, TPortionInfo::TConstPtr>& GetPortionsToRemove() const {
        return PortionsToRemove;
    }

    ui32 GetSize() const {
        return Portions.size();
    }

    bool HasPortions() const {
        return Portions.size();
    }

    void AddPortions(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
        for (auto&& i : portions) {
            AFL_VERIFY(i);
            AFL_VERIFY(Portions.emplace(i->GetAddress(), i).second)("portion_id", i->GetPortionId());
        }
    }

    void SetTargetCompactionLevel(const ui64 level) {
        TargetCompactionLevel = level;
    }
};

class TColumnEngineChanges {
public:
    enum class EStage : ui32 {
        Created = 0,
        Started,
        Constructed,
        Compiled,
        Written,
        Finished,
        Aborted
    };

private:
    EStage Stage = EStage::Created;
    std::shared_ptr<NDataLocks::TManager::TGuard> LockGuard;
    TString AbortedReason;
    const TString TaskIdentifier = TGUID::CreateTimebased().AsGuidString();
    std::shared_ptr<const TAtomicCounter> ActivityFlag;

protected:
    std::optional<TDataAccessorsResult> FetchedDataAccessors;
    virtual NDataLocks::ELockCategory GetLockCategory() const = 0;
    virtual void DoDebugString(TStringOutput& out) const = 0;
    virtual void DoCompile(TFinalizationContext& context) = 0;
    virtual void DoOnAfterCompile() {
    }
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) = 0;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) = 0;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) = 0;
    virtual bool NeedConstruction() const {
        return true;
    }
    virtual void DoStart(NColumnShard::TColumnShard& context) = 0;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept = 0;
    virtual void OnAbortEmergency() {
    }

    TBlobsAction BlobsAction;

    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const = 0;

    virtual ui64 DoCalcMemoryForUsage() const = 0;
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const = 0;
    std::shared_ptr<NDataLocks::ILock> BuildDataLock() const {
        return DoBuildDataLock();
    }

    std::shared_ptr<TDataAccessorsRequest> PortionsToAccess = std::make_shared<TDataAccessorsRequest>(TaskIdentifier);
    virtual void OnDataAccessorsInitialized(const TDataAccessorsInitializationContext& context) = 0;

public:
    bool IsActive() const {
        return !ActivityFlag || ActivityFlag->Val();
    }

    void SetActivityFlag(const std::shared_ptr<const TAtomicCounter>& flag) {
        AFL_VERIFY(!ActivityFlag);
        ActivityFlag = flag;
    }

    std::shared_ptr<TDataAccessorsRequest> ExtractDataAccessorsRequest() const {
        AFL_VERIFY(!!PortionsToAccess);
        return std::move(PortionsToAccess);
    }

    const TPortionDataAccessor& GetPortionDataAccessor(const ui64 portionId) const {
        AFL_VERIFY(FetchedDataAccessors);
        return FetchedDataAccessors->GetPortionAccessorVerified(portionId);
    }

    std::vector<TPortionDataAccessor> GetPortionDataAccessors(const std::vector<TPortionInfo::TConstPtr>& portions) const {
        AFL_VERIFY(FetchedDataAccessors);
        std::vector<TPortionDataAccessor> result;
        for (auto&& i : portions) {
            result.emplace_back(GetPortionDataAccessor(i->GetPortionId()));
        }
        return result;
    }

    void SetFetchedDataAccessors(TDataAccessorsResult&& result, const TDataAccessorsInitializationContext& context) {
        AFL_VERIFY(!FetchedDataAccessors);
        FetchedDataAccessors = std::move(result);
        OnDataAccessorsInitialized(context);
    }

    class IMemoryPredictor {
    public:
        virtual ui64 AddPortion(const TPortionInfo::TConstPtr& portionInfo) = 0;
        virtual ~IMemoryPredictor() = default;
    };

    virtual bool NeedDiskWriteLimiter() const {
        return false;
    }

    void OnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context);

    ui64 CalcMemoryForUsage() const {
        return DoCalcMemoryForUsage();
    }

    TString GetTaskIdentifier() const {
        return TaskIdentifier;
    }

    TBlobsAction& MutableBlobsAction() {
        return BlobsAction;
    }

    const TBlobsAction& GetBlobsAction() const {
        return BlobsAction;
    }

    TColumnEngineChanges(const std::shared_ptr<IStoragesManager>& storagesManager, const NBlobOperations::EConsumer consumerId)
        : BlobsAction(storagesManager, consumerId) {
    }

    TConclusionStatus ConstructBlobs(TConstructionContext& context) noexcept;
    virtual ~TColumnEngineChanges();

    bool IsAborted() const {
        return Stage == EStage::Aborted;
    }

    void StartEmergency();
    void AbortEmergency(const TString& reason);

    void Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context);
    void Start(NColumnShard::TColumnShard& self);

    virtual ui32 GetWritePortionsCount() const = 0;
    virtual TWritePortionInfoWithBlobsResult* GetWritePortionInfo(const ui32 index) = 0;
    virtual bool NeedWritePortion(const ui32 index) const = 0;

    void WriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context);
    void WriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context);

    void Compile(TFinalizationContext& context) noexcept;

    NBlobOperations::NRead::TCompositeReadBlobs Blobs;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

    std::vector<std::shared_ptr<IBlobsReadingAction>> GetReadingActions() const {
        auto result = BlobsAction.GetReadingActions();
        //        Y_ABORT_UNLESS(result.size());
        return result;
    }
    virtual TString TypeString() const = 0;
    TString DebugString() const;

    ui64 TotalBlobsSize() const {
        return Blobs.GetTotalBlobsSize();
    }
};

}   // namespace NKikimr::NOlap
