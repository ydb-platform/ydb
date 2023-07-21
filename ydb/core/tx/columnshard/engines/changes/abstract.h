#pragma once
#include "mark.h"
#include <ydb/core/tx/columnshard/engines/columns_table.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

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

struct TCompactionLimits {
    static constexpr const ui64 MIN_GOOD_BLOB_SIZE = 256 * 1024; // some BlobStorage constant
    static constexpr const ui64 MAX_BLOB_SIZE = 8 * 1024 * 1024; // some BlobStorage constant
    static constexpr const ui64 EVICT_HOT_PORTION_BYTES = 1 * 1024 * 1024;
    static constexpr const ui64 DEFAULT_EVICTION_BYTES = 64 * 1024 * 1024;
    static constexpr const ui64 MAX_BLOBS_TO_DELETE = 10000;

    static constexpr const ui64 OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID = 1024 * MAX_BLOB_SIZE;
    static constexpr const ui64 WARNING_INSERT_TABLE_SIZE_BY_PATH_ID = 0.3 * OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID;
    static constexpr const ui64 WARNING_INSERT_TABLE_COUNT_BY_PATH_ID = 100;

    static constexpr const ui64 OVERLOAD_GRANULE_SIZE = 20 * MAX_BLOB_SIZE;
    static constexpr const ui64 WARNING_OVERLOAD_GRANULE_SIZE = 0.25 * OVERLOAD_GRANULE_SIZE;

    static constexpr const ui64 WARNING_INSERTED_PORTIONS_SIZE = 0.5 * WARNING_OVERLOAD_GRANULE_SIZE;
    static constexpr const ui64 WARNING_INSERTED_PORTIONS_COUNT = 100;
    static constexpr const TDuration CompactionTimeout = TDuration::Minutes(3);

    ui32 GoodBlobSize{MIN_GOOD_BLOB_SIZE};
    ui32 GranuleBlobSplitSize{MAX_BLOB_SIZE};

    ui32 InGranuleCompactSeconds = 2 * 60; // Trigger in-granule compaction to guarantee no PK intersections

    ui32 GranuleOverloadSize = OVERLOAD_GRANULE_SIZE;
    ui32 GranuleSizeForOverloadPrevent = WARNING_OVERLOAD_GRANULE_SIZE;
    ui32 GranuleIndexedPortionsSizeLimit = WARNING_INSERTED_PORTIONS_SIZE;
    ui32 GranuleIndexedPortionsCountLimit = WARNING_INSERTED_PORTIONS_COUNT;
};

struct TPortionEvictionFeatures {
    const TString TargetTierName;
    const ui64 PathId;      // portion path id for cold-storage-key construct
    bool NeedExport = false;
    bool DataChanges = true;

    TPortionEvictionFeatures(const TString& targetTierName, const ui64 pathId, const bool needExport)
        : TargetTierName(targetTierName)
        , PathId(pathId)
        , NeedExport(needExport)
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

};

class TWriteIndexCompleteContext: TNonCopyable, public TChangesFinishContext {
public:
    const TActorContext& ActorContext;
    const ui32 BlobsWritten;
    const ui64 BytesWritten;
    const bool FinishedSuccessfully;
    const TDuration Duration;
    NColumnShard::TBackgroundActivity& TriggerActivity;
    TWriteIndexCompleteContext(const TActorContext& actorContext, const ui32 blobsWritten, const ui64 bytesWritten, const bool finishedSuccessfully, const TDuration d,
        NColumnShard::TBackgroundActivity& triggerActivity)
        : ActorContext(actorContext)
        , BlobsWritten(blobsWritten)
        , BytesWritten(bytesWritten)
        , FinishedSuccessfully(finishedSuccessfully)
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

class TGranuleMeta;

class TColumnEngineChanges {
public:
    enum class EStage: ui32 {
        Created = 0,
        Started,
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
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) = 0;
    virtual void OnChangesApplyFailed(const TString& /*errorMessage*/) {
    }
    virtual void OnChangesApplyFinished() {
    }
    virtual void DoAbort() {

    }
    virtual void DoStart(NColumnShard::TColumnShard& self) = 0;
public:
    void AbortEmergency() {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "AbortEmergency");
        Stage = EStage::Aborted;
    }

    void Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
        Y_VERIFY(Stage != EStage::Finished && Stage != EStage::Created && Stage != EStage::Aborted);
        Stage = EStage::Aborted;
        DoAbort();
        DoOnFinish(self, context);
    }

    virtual ~TColumnEngineChanges() {
        Y_VERIFY(Stage == EStage::Created || Stage == EStage::Finished || Stage == EStage::Aborted);
    }

    void Start(NColumnShard::TColumnShard& self) {
        Y_VERIFY(Stage == EStage::Created);
        DoStart(self);
        Stage = EStage::Started;
    }

    void StartEmergency() {
        Y_VERIFY(Stage == EStage::Created);
        Stage = EStage::Started;
    }

    bool ApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) {
        Y_VERIFY(Stage != EStage::Aborted);
        if ((ui32)Stage >= (ui32)EStage::Applied) {
            return true;
        }
        Y_VERIFY(Stage == EStage::Compiled);

        if (!DoApplyChanges(self, context, dryRun)) {
            if (dryRun) {
                OnChangesApplyFailed("problems on apply");
            }
            Y_VERIFY(dryRun);
            return false;
        } else if (!dryRun) {
            OnChangesApplyFinished();
            Stage = EStage::Applied;
        }
        return true;
    }

    virtual ui32 GetWritePortionsCount() const = 0;
    virtual const TPortionInfo& GetWritePortionInfo(const ui32 index) const = 0;
    virtual bool NeedWritePortion(const ui32 index) const = 0;
    virtual void UpdateWritePortionInfo(const ui32 index, const TPortionInfo& info) = 0;

    void WriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
        Y_VERIFY(Stage != EStage::Aborted);
        if ((ui32)Stage >= (ui32)EStage::Written) {
            return;
        }
        Y_VERIFY(Stage == EStage::Applied);

        DoWriteIndex(self, context);
        Stage = EStage::Written;
    }
    void WriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
        Y_VERIFY(Stage == EStage::Aborted || Stage == EStage::Written);
        if (Stage == EStage::Aborted) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("stage", Stage);
            return;
        }
        if (Stage == EStage::Written) {
            Stage = EStage::Finished;
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("type", TypeString())("success", context.FinishedSuccessfully);
            DoWriteIndexComplete(self, context);
            DoOnFinish(self, context);
        }
    }

    void Compile(TFinalizationContext& context) noexcept {
        Y_VERIFY(Stage != EStage::Aborted);
        if ((ui32)Stage >= (ui32)EStage::Compiled) {
            return;
        }
        Y_VERIFY(Stage == EStage::Started);

        DoCompile(context);

        Stage = EStage::Compiled;
    }

    void SetBlobs(THashMap<TBlobRange, TString>&& blobs) {
        Y_VERIFY(!blobs.empty());
        Blobs = std::move(blobs);
    }

    TSnapshot InitSnapshot = TSnapshot::Zero();
    THashMap<TBlobRange, TString> Blobs;
    bool NeedRepeat{false};

    virtual THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GetGroupedBlobRanges() const = 0;
    virtual TString TypeString() const = 0;
    TString DebugString() const;

    virtual const TGranuleMeta* GetGranuleMeta() const {
        return nullptr;
    }

    ui64 TotalBlobsSize() const {
        ui64 size = 0;
        for (const auto& [_, blob] : Blobs) {
            size += blob.size();
        }
        return size;
    }

    /// Returns blob-ranges grouped by blob-id.
    static THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GroupedBlobRanges(const std::vector<TPortionInfo>& portions) {
        Y_VERIFY(portions.size());

        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> sameBlobRanges;
        for (const auto& portionInfo : portions) {
            Y_VERIFY(!portionInfo.Empty());

            for (const auto& rec : portionInfo.Records) {
                sameBlobRanges[rec.BlobRange.BlobId].push_back(rec.BlobRange);
            }
        }
        return sameBlobRanges;
    }

    /// Returns blob-ranges grouped by blob-id.
    static THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GroupedBlobRanges(const std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>>& portions) {
        Y_VERIFY(portions.size());

        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> sameBlobRanges;
        for (const auto& [portionInfo, _] : portions) {
            Y_VERIFY(!portionInfo.Empty());

            for (const auto& rec : portionInfo.Records) {
                sameBlobRanges[rec.BlobRange.BlobId].push_back(rec.BlobRange);
            }
        }
        return sameBlobRanges;
    }
};

}
