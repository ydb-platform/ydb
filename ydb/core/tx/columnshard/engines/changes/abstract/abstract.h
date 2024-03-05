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
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/with_blobs.h>
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
class TBackgroundActivity;
}

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class TVersionedIndex;
class TPortionInfoWithBlobs;

struct TPortionEvictionFeatures {
    const TString TargetTierName;
    const ui64 PathId;      // portion path id for cold-storage-key construct

    TPortionEvictionFeatures(const TString& targetTierName, const ui64 pathId)
        : TargetTierName(targetTierName)
        , PathId(pathId)
    {}
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
    NColumnShard::TBackgroundActivity& TriggerActivity;
    TColumnEngineForLogs& EngineLogs;
    TWriteIndexCompleteContext(const TActorContext& actorContext, const ui32 blobsWritten, const ui64 bytesWritten
        , const TDuration d, NColumnShard::TBackgroundActivity& triggerActivity, TColumnEngineForLogs& engineLogs)
        : ActorContext(actorContext)
        , BlobsWritten(blobsWritten)
        , BytesWritten(bytesWritten)
        , Duration(d)
        , TriggerActivity(triggerActivity)
        , EngineLogs(engineLogs)
    {

    }
};

class TConstructionContext: TNonCopyable {
public:
    const TVersionedIndex& SchemaVersions;
    const NColumnShard::TIndexationCounters Counters;

    TConstructionContext(const TVersionedIndex& schemaVersions, const NColumnShard::TIndexationCounters& counters)
        : SchemaVersions(schemaVersions)
        , Counters(counters) {

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
protected:
    virtual void DoDebugString(TStringOutput& out) const = 0;
    virtual void DoCompile(TFinalizationContext& context) = 0;
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

    TColumnEngineChanges(const std::shared_ptr<IStoragesManager>& storagesManager, const TString& consumerId)
        : BlobsAction(storagesManager, consumerId)
    {

    }

    TConclusionStatus ConstructBlobs(TConstructionContext& context) noexcept;
    virtual ~TColumnEngineChanges();

    bool IsAborted() const {
        return Stage == EStage::Aborted;
    }

    void StartEmergency();
    void AbortEmergency();

    void Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context);
    void Start(NColumnShard::TColumnShard& self);

    virtual ui32 GetWritePortionsCount() const = 0;
    virtual TPortionInfoWithBlobs* GetWritePortionInfo(const ui32 index) = 0;
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
