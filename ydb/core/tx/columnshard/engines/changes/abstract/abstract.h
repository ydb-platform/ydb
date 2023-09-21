#pragma once
#include "mark.h"
#include "settings.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/columns_table.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/with_blobs.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/stream/str.h>
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
    bool DataChanges = true;
    const std::shared_ptr<IBlobsStorageOperator> StorageOperator;

    TPortionEvictionFeatures(const TString& targetTierName, const ui64 pathId, const std::shared_ptr<IBlobsStorageOperator>& storageOperator)
        : TargetTierName(targetTierName)
        , PathId(pathId)
        , StorageOperator(storageOperator)
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
    NTabletFlatExecutor::TTransactionContext& Txc;
    std::shared_ptr<NColumnShard::TBlobManagerDb> BlobManagerDb;
    IDbWrapper& DBWrapper;
    TWriteIndexContext(NTabletFlatExecutor::TTransactionContext& txc, IDbWrapper& dbWrapper);
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
    TWriteIndexCompleteContext(const TActorContext& actorContext, const ui32 blobsWritten, const ui64 bytesWritten
        , const TDuration d, NColumnShard::TBackgroundActivity& triggerActivity)
        : ActorContext(actorContext)
        , BlobsWritten(blobsWritten)
        , BytesWritten(bytesWritten)
        , Duration(d)
        , TriggerActivity(triggerActivity)
    {

    }
};

class TApplyChangesContext: TNonCopyable {
public:
    IDbWrapper& DB;
    const TSnapshot Snapshot;
    TApplyChangesContext(IDbWrapper& db, const TSnapshot& snapshot)
        : DB(db)
        , Snapshot(snapshot) {

    }
};

class TConstructionContext: TNonCopyable {
public:
    const TVersionedIndex& SchemaVersions;
    const NColumnShard::TIndexationCounters Counters;

    TConstructionContext(const TVersionedIndex& schemaVersions, const NColumnShard::TIndexationCounters counters)
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
        Applied,
        Written,
        Finished,
        Aborted
    };
private:
    EStage Stage = EStage::Created;
protected:
    virtual void DoDebugString(TStringOutput& out) const = 0;
    virtual void DoCompile(TFinalizationContext& context) = 0;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) = 0;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) = 0;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) = 0;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) = 0;
    virtual bool NeedConstruction() const {
        return true;
    }
    virtual void DoStart(NColumnShard::TColumnShard& self) = 0;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept = 0;
    virtual void OnAbortEmergency() {

    }

    TBlobsAction BlobsAction;

    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const = 0;
public:
    TBlobsAction& GetBlobsAction() {
        return BlobsAction;
    }

    TColumnEngineChanges(const std::shared_ptr<IStoragesManager>& storagesManager)
        : BlobsAction(storagesManager)
    {

    }

    TConclusionStatus ConstructBlobs(TConstructionContext& context) noexcept;
    virtual ~TColumnEngineChanges();

    bool IsAborted() const {
        return Stage == EStage::Aborted;
    }

    virtual THashSet<TPortionAddress> GetTouchedPortions() const = 0;

    void StartEmergency();
    void AbortEmergency();

    void Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context);
    void Start(NColumnShard::TColumnShard& self);
    bool ApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context);

    virtual ui32 GetWritePortionsCount() const = 0;
    virtual TPortionInfoWithBlobs* GetWritePortionInfo(const ui32 index) = 0;
    virtual bool NeedWritePortion(const ui32 index) const = 0;

    void WriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context);
    void WriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context);

    void Compile(TFinalizationContext& context) noexcept;

    void SetBlobs(THashMap<TBlobRange, TString>&& blobs) {
        Y_VERIFY(!blobs.empty());
        Blobs = std::move(blobs);
    }

    THashMap<TBlobRange, TString> Blobs;

    std::vector<std::shared_ptr<IBlobsReadingAction>> GetReadingActions() const {
        auto result = BlobsAction.GetReadingActions();
        Y_VERIFY(result.size());
        return result;
    }
    virtual TString TypeString() const = 0;
    TString DebugString() const;

    ui64 TotalBlobsSize() const {
        ui64 size = 0;
        for (const auto& [_, blob] : Blobs) {
            size += blob.size();
        }
        return size;
    }

};

}
