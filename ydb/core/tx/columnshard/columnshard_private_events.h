#pragma once

#include "defs.h"

#include "blobs_action/abstract/gc.h"

#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/resource_subscriber/container.h>
#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/priorities/usage/abstract.h>

namespace NKikimr::NOlap::NReader {
class IApplyAction;
}

namespace NKikimr::NOlap {
class IBlobsWritingAction;
class TPortionInfo;
class TPortionInfoConstructor;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NGeneralCache {
class TColumnDataCachePolicy;
class TGlobalColumnAddress;
}   // namespace NKikimr::NOlap::NGeneralCache

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

        EvWritingPortionsAddDataToBuffer,
        EvWritingPortionsFlushBuffer,

        EvExportWritingFinished,
        EvExportWritingFailed,
        EvExportCursorSaved,
        EvExportSaveCursor,

        EvTaskProcessedResult,
        EvPingSnapshotsUsage,
        EvWritePortionResult,
        EvStartCompaction,

        EvRegisterGranuleDataAccessor,
        EvUnregisterGranuleDataAccessor,
        EvAskTabletDataAccessors,
        EvAskServiceDataAccessors,
        EvAddPortionDataAccessor,
        EvRemovePortionDataAccessor,
        EvClearCacheDataAccessor,
        EvMetadataAccessorsInfo,
        EvAskColumnData,

        EvRequestFilter,
        EvFilterConstructionResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    class TEvMetadataAccessorsInfo: public NActors::TEventLocal<TEvMetadataAccessorsInfo, EvMetadataAccessorsInfo> {
    private:
        const std::shared_ptr<NOlap::IMetadataAccessorResultProcessor> Processor;
        const ui64 Generation;
        std::optional<NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>> Result;

    public:
        const std::shared_ptr<NOlap::IMetadataAccessorResultProcessor>& GetProcessor() const {
            return Processor;
        }
        ui64 GetGeneration() const {
            return Generation;
        }
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult> ExtractResult() {
            AFL_VERIFY(Result);
            auto result = std::move(*Result);
            Result.reset();
            return result;
        }

        TEvMetadataAccessorsInfo(const std::shared_ptr<NOlap::IMetadataAccessorResultProcessor>& processor, const ui64 gen,
            NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result)
            : Processor(processor)
            , Generation(gen)
            , Result(std::move(result)) {
        }
    };

    class TEvAskTabletDataAccessors
        : public NActors::TEventLocal<TEvAskTabletDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskTabletDataAccessors> {
    private:
        using TPortions = THashMap<TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer>;
        YDB_ACCESSOR_DEF(TPortions, Portions);
        YDB_READONLY_DEF(std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>, Callback);

    public:
        explicit TEvAskTabletDataAccessors(TPortions&& portions, const std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>& callback)
            : Portions(std::move(portions))
            , Callback(callback) {
        }
    };

    class TEvAskColumnData: public NActors::TEventLocal<TEvAskColumnData, NColumnShard::TEvPrivate::EEv::EvAskColumnData> {
    public:
        class TPortionRequest {
        private:
            NOlap::TPortionAddress Portion;
            YDB_READONLY_DEF(NOlap::NBlobOperations::EConsumer, Consumer);

        public:
            TPortionRequest(const NOlap::TPortionAddress& portion, const NOlap::NBlobOperations::EConsumer consumer)
                : Portion(portion)
                , Consumer(consumer)
            {
            }

            operator size_t() const {
                ui64 h = 0;
                h = CombineHashes(h, THash<NOlap::TPortionAddress>()(Portion));
                h = CombineHashes(h, (size_t)Consumer);
                return h;
            }
            bool operator==(const TPortionRequest& other) const {
                return Portion == other.Portion && Consumer == other.Consumer;
            }

            NOlap::TPortionAddress GetPortionAddress() const {
                return Portion;
            }
        };

    private:
        using TCallback = NKikimr::NGeneralCache::NSource::IObjectsProcessor<NOlap::NGeneralCache::TColumnDataCachePolicy>;
        using TColumnIdsByRequest = THashMap<TPortionRequest, std::set<ui32>>;
        YDB_READONLY_DEF(TColumnIdsByRequest, Requests);
        YDB_READONLY_DEF(std::shared_ptr<TCallback>, Callback);

    public:
        explicit TEvAskColumnData(TColumnIdsByRequest&& requests, const std::shared_ptr<TCallback>& callback)
            : Requests(std::move(requests))
            , Callback(callback)
        {
            AFL_VERIFY(Callback);
            AFL_VERIFY(requests.size());
        }
    };

    class TEvStartCompaction: public NActors::TEventLocal<TEvStartCompaction, EvStartCompaction> {
    private:
        YDB_READONLY_DEF(std::shared_ptr<NPrioritiesQueue::TAllocationGuard>, Guard);

    public:
        TEvStartCompaction(const std::shared_ptr<NPrioritiesQueue::TAllocationGuard>& g)
            : Guard(g) {
        }
    };

    class TEvTaskProcessedResult: public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult> {
    private:
        TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>> Result;
        TCounterGuard ScanCounter;

    public:
        TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>>& MutableResult() {
            return Result;
        }

        TEvTaskProcessedResult(
            TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>>&& result, TCounterGuard&& scanCounters)
            : Result(std::move(result))
            , ScanCounter(std::move(scanCounters)) {
        }
    };

    struct TEvTieringModified: public TEventLocal<TEvTieringModified, EvTieringModified> {};

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
            : Changes(changes) {
        }

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
    struct TEvWriteIndex: public TEventLocal<TEvWriteIndex, EvWriteIndex> {
        std::shared_ptr<NOlap::TColumnEngineChanges> IndexChanges;
        bool GranuleCompaction{ false };
        TUsage ResourceUsage;
        bool CacheData{ false };
        TDuration Duration;
        TBlobPutResult::TPtr PutResult;
        TString ErrorMessage;

        TEvWriteIndex(std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges, bool cacheData)
            : IndexChanges(indexChanges)
            , CacheData(cacheData) {
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

    struct TEvScanStats: public TEventLocal<TEvScanStats, EvScanStats> {
        TEvScanStats(ui64 rows, ui64 bytes)
            : Rows(rows)
            , Bytes(bytes) {
        }
        ui64 Rows;
        ui64 Bytes;
    };

    struct TEvReadFinished: public TEventLocal<TEvReadFinished, EvReadFinished> {
        explicit TEvReadFinished(ui64 requestCookie, ui64 txId = 0)
            : RequestCookie(requestCookie)
            , TxId(txId) {
        }

        ui64 RequestCookie;
        ui64 TxId;
    };

    struct TEvPeriodicWakeup: public TEventLocal<TEvPeriodicWakeup, EvPeriodicWakeup> {
        TEvPeriodicWakeup(bool manual = false)
            : Manual(manual) {
        }

        bool Manual;
    };

    struct TEvPingSnapshotsUsage: public TEventLocal<TEvPingSnapshotsUsage, EvPingSnapshotsUsage> {
        TEvPingSnapshotsUsage() = default;
    };

    class TEvWriteBlobsResult: public TEventLocal<TEvWriteBlobsResult, EvWriteBlobsResult> {
    public:
        enum EErrorClass {
            Internal,
            Request,
            ConstraintViolation
        };

    private:
        NColumnShard::TBlobPutResult::TPtr PutResult;
        NOlap::TWritingBuffer WritesBuffer;
        YDB_READONLY_DEF(TString, ErrorMessage);
        YDB_ACCESSOR(EErrorClass, ErrorClass, EErrorClass::Internal);

    public:
        NKikimrDataEvents::TEvWriteResult::EStatus GetWriteResultStatus() const {
            switch (ErrorClass) {
                case EErrorClass::Internal:
                    return NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR;
                case EErrorClass::Request:
                    return NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST;
                case EErrorClass::ConstraintViolation:
                    return NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION;
            }
        }

        static std::unique_ptr<TEvWriteBlobsResult> Error(
            const NKikimrProto::EReplyStatus status, NOlap::TWritingBuffer&& writesBuffer, const TString& error, const EErrorClass errorClass) {
            std::unique_ptr<TEvWriteBlobsResult> result =
                std::make_unique<TEvWriteBlobsResult>(std::make_shared<NColumnShard::TBlobPutResult>(status), std::move(writesBuffer));
            result->ErrorMessage = error;
            result->ErrorClass = errorClass;
            return result;
        }

        TEvWriteBlobsResult(const NColumnShard::TBlobPutResult::TPtr& putResult, NOlap::TWritingBuffer&& writesBuffer)
            : PutResult(putResult)
            , WritesBuffer(std::move(writesBuffer)) {
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

}   // namespace NKikimr::NColumnShard
