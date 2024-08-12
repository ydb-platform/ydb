#pragma once
#include "settings.h"
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/locks/composite.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/stream/str.h>
#include <util/generic/guid.h>
#include <compare>

namespace NKikimr::NTabletFlatExecutor {
class TTransactionContext;
}

namespace NKikimr::NColumnShard {
class TBlobManagerDb;
class TColumnShard;
}

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
    TPortionEvictionFeatures(const std::shared_ptr<ISnapshotSchema>& currentScheme, const std::shared_ptr<ISnapshotSchema>& targetScheme, const TString& currentTierName)
        : CurrentScheme(currentScheme)
        , TargetScheme(targetScheme)
        , CurrentTierName(currentTierName)
    {
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
            RWAddress = NActualizer::TRWAddress(CurrentScheme->GetIndexInfo().GetUsedStorageIds(CurrentTierName), TargetScheme->GetIndexInfo().GetUsedStorageIds(*TargetTierName));
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
    TWriteIndexContext(NTable::TDatabase* db, IDbWrapper& dbWrapper, TColumnEngineForLogs& engineLogs);
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
    TWriteIndexCompleteContext(const TActorContext& actorContext, const ui32 blobsWritten, const ui64 bytesWritten
        , const TDuration d, TColumnEngineForLogs& engineLogs)
        : ActorContext(actorContext)
        , BlobsWritten(blobsWritten)
        , BytesWritten(bytesWritten)
        , Duration(d)
        , EngineLogs(engineLogs)
    {

    }
};

class TConstructionContext: TNonCopyable {
public:
    const TVersionedIndex& SchemaVersions;
    const NColumnShard::TIndexationCounters Counters;
    const NOlap::TSnapshot LastCommittedTx;

    TConstructionContext(const TVersionedIndex& schemaVersions, const NColumnShard::TIndexationCounters& counters, const NOlap::TSnapshot& lastCommittedTx)
        : SchemaVersions(schemaVersions)
        , Counters(counters)
        , LastCommittedTx(lastCommittedTx)
    {

    }
};

class TGranuleMeta;

class TColumnEngineChanges {
public:
    enum class EStage: ui32 {
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

protected:
    virtual void DoDebugString(TStringOutput& out) const = 0;
    virtual void DoCompile(TFinalizationContext& context) = 0;
    virtual void DoOnAfterCompile() {}
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) = 0;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) = 0;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) = 0;
    virtual bool NeedConstruction() const {
        return true;
    }
    virtual void DoStart(NColumnShard::TColumnShard& self) = 0;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept = 0;
    virtual void OnAbortEmergency() {
    }

    TBlobsAction BlobsAction;

    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const = 0;

    const TString TaskIdentifier = TGUID::Create().AsGuidString();
    virtual ui64 DoCalcMemoryForUsage() const = 0;
    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const = 0;
    std::shared_ptr<NDataLocks::ILock> BuildDataLock() const {
        return DoBuildDataLock();
    }

public:
    class IMemoryPredictor {
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) = 0;
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
        : BlobsAction(storagesManager, consumerId)
    {

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
        Y_ABORT_UNLESS(result.size());
        return result;
    }
    virtual TString TypeString() const = 0;
    TString DebugString() const;

    ui64 TotalBlobsSize() const {
        return Blobs.GetTotalBlobsSize();
    }

};

}
