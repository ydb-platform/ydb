#pragma once

#include "blobs_action/abstract/gc.h"
#include "defs.h"

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/formats/arrow/special_keys.h>

namespace NKikimr::NOlap::NReader {
class IApplyAction;
}

namespace NKikimr::NColumnShard {

struct TEvPrivate {
    enum EEv {
        EvIndexing = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvWriteIndex,
        EvScanStats,
        EvReadFinished,
        EvPeriodicWakeup,
        EvEviction,
        EvS3Settings,
        EvExport,
        EvForget,
        EvGetExported,
        EvWriteBlobsResult,
        EvStartReadTask,
        EvWriteDraft,
        EvGarbageCollectionFinished,
        EvTieringModified,
        EvStartResourceUsageTask,
        EvNormalizerResult,

        EvWritingAddDataToBuffer,
        EvWritingFlushBuffer,

        EvExportWritingFinished,
        EvExportWritingFailed,
        EvExportCursorSaved,
        EvExportSaveCursor,

        EvTaskProcessedResult,
        EvPingSnapshotsUsage,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    class TEvTaskProcessedResult: public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult> {
    private:
        TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>> Result;

    public:
        TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>> ExtractResult() {
            return std::move(Result);
        }

        TEvTaskProcessedResult(const TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>>& result)
            : Result(result) {
        }
    };

    struct TEvTieringModified: public TEventLocal<TEvTieringModified, EvTieringModified> {
    };

    struct TEvWriteDraft: public TEventLocal<TEvWriteDraft, EvWriteDraft> {
        const std::shared_ptr<IWriteController> WriteController;
        TEvWriteDraft(std::shared_ptr<IWriteController> controller)
            : WriteController(controller) {

        }
    };

    class TEvNormalizerResult: public TEventLocal<TEvNormalizerResult, EvNormalizerResult> {
        NOlap::INormalizerChanges::TPtr Changes;
    public:
        TEvNormalizerResult(NOlap::INormalizerChanges::TPtr changes)
            : Changes(changes)
        {}

        NOlap::INormalizerChanges::TPtr GetChanges() const {
            Y_ABORT_UNLESS(!!Changes);
            return Changes;
        }
    };

    struct TEvGarbageCollectionFinished: public TEventLocal<TEvGarbageCollectionFinished, EvGarbageCollectionFinished> {
        const std::shared_ptr<NOlap::IBlobsGCAction> Action;
        TEvGarbageCollectionFinished(const std::shared_ptr<NOlap::IBlobsGCAction>& action)
            : Action(action) {

        }
    };

    /// Common event for Indexing and GranuleCompaction: write index data in TTxWriteIndex transaction.
    struct TEvWriteIndex : public TEventLocal<TEvWriteIndex, EvWriteIndex> {
        std::shared_ptr<NOlap::TVersionedIndex> IndexInfo;
        std::shared_ptr<NOlap::TColumnEngineChanges> IndexChanges;
        bool GranuleCompaction{false};
        TUsage ResourceUsage;
        bool CacheData{false};
        TDuration Duration;
        TBlobPutResult::TPtr PutResult;

        TEvWriteIndex(const std::shared_ptr<NOlap::TVersionedIndex>& indexInfo,
            std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges,
            bool cacheData)
            : IndexInfo(indexInfo)
            , IndexChanges(indexChanges)
            , CacheData(cacheData)
        {
            PutResult = std::make_shared<TBlobPutResult>(NKikimrProto::UNKNOWN);
        }

        const TBlobPutResult& GetPutResult() const {
            Y_ABORT_UNLESS(PutResult);
            return *PutResult;
        }

        NKikimrProto::EReplyStatus GetPutStatus() const {
            Y_ABORT_UNLESS(PutResult);
            return PutResult->GetPutStatus();
        }

        void SetPutStatus(const NKikimrProto::EReplyStatus& status) {
            Y_ABORT_UNLESS(PutResult);
            PutResult->SetPutStatus(status);
        }
    };

    struct TEvScanStats : public TEventLocal<TEvScanStats, EvScanStats> {
        TEvScanStats(ui64 rows, ui64 bytes) : Rows(rows), Bytes(bytes) {}
        ui64 Rows;
        ui64 Bytes;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        explicit TEvReadFinished(ui64 requestCookie, ui64 txId = 0)
            : RequestCookie(requestCookie)
            , TxId(txId) {
        }

        ui64 RequestCookie;
        ui64 TxId;
    };

    struct TEvPeriodicWakeup : public TEventLocal<TEvPeriodicWakeup, EvPeriodicWakeup> {
        TEvPeriodicWakeup(bool manual = false)
            : Manual(manual)
        {}

        bool Manual;
    };

    struct TEvPingSnapshotsUsage: public TEventLocal<TEvPingSnapshotsUsage, EvPingSnapshotsUsage> {
        TEvPingSnapshotsUsage() = default;
    };

    class TEvWriteBlobsResult: public TEventLocal<TEvWriteBlobsResult, EvWriteBlobsResult> {
    private:
        NColumnShard::TBlobPutResult::TPtr PutResult;
        NOlap::TWritingBuffer WritesBuffer;
        YDB_READONLY_DEF(TString, ErrorMessage);
    public:
        
        static std::unique_ptr<TEvWriteBlobsResult> Error(const NKikimrProto::EReplyStatus status, NOlap::TWritingBuffer&& writesBuffer, const TString& error) {
            std::unique_ptr<TEvWriteBlobsResult> result = std::make_unique<TEvWriteBlobsResult>(std::make_shared<NColumnShard::TBlobPutResult>(status), 
                std::move(writesBuffer));
            result->ErrorMessage = error;
            return result;
        }

        TEvWriteBlobsResult(const NColumnShard::TBlobPutResult::TPtr& putResult, NOlap::TWritingBuffer&& writesBuffer)
            : PutResult(putResult)
            , WritesBuffer(std::move(writesBuffer))
        {
            Y_ABORT_UNLESS(PutResult);
        }

        const NColumnShard::TBlobPutResult& GetPutResult() const {
            return *PutResult;
        }

        const NOlap::TWritingBuffer& GetWritesBuffer() const {
            return WritesBuffer;
        }

        NOlap::TWritingBuffer& MutableWritesBuffer() {
            return WritesBuffer;
        }
    };
};

}
