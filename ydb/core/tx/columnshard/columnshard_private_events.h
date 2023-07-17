#pragma once

#include "blob_manager.h"
#include "defs.h"

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/ev_write/write_data.h>

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
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    /// Common event for Indexing and GranuleCompaction: write index data in TTxWriteIndex transaction.
    struct TEvWriteIndex : public TEventLocal<TEvWriteIndex, EvWriteIndex> {
        NOlap::TVersionedIndex IndexInfo;
        THashMap<ui64, NKikimr::NOlap::TTiering> Tiering;
        std::shared_ptr<NOlap::TColumnEngineChanges> IndexChanges;
        THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CachedBlobs;
        std::vector<TString> Blobs;
        bool GranuleCompaction{false};
        TBlobBatch BlobBatch;
        TUsage ResourceUsage;
        bool CacheData{false};
        TDuration Duration;
        TBlobPutResult::TPtr PutResult;

        TEvWriteIndex(NOlap::TVersionedIndex&& indexInfo,
            std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges,
            bool cacheData,
            THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>>&& cachedBlobs = {})
            : IndexInfo(std::move(indexInfo))
            , IndexChanges(indexChanges)
            , CachedBlobs(std::move(cachedBlobs))
            , CacheData(cacheData)
        {
            PutResult = std::make_shared<TBlobPutResult>(NKikimrProto::UNKNOWN);
        }

        TEvWriteIndex& SetTiering(const THashMap<ui64, NKikimr::NOlap::TTiering>& tiering) {
            Tiering = tiering;
            return *this;
        }

        const TBlobPutResult& GetPutResult() const {
            Y_VERIFY(PutResult);
            return *PutResult;
        }

        NKikimrProto::EReplyStatus GetPutStatus() const {
            Y_VERIFY(PutResult);
            return PutResult->GetPutStatus();
        }

        void SetPutStatus(const NKikimrProto::EReplyStatus& status) {
            Y_VERIFY(PutResult);
            PutResult->SetPutStatus(status);
        }
    };

    struct TEvIndexing : public TEventLocal<TEvIndexing, EvIndexing> {
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;

        explicit TEvIndexing(std::unique_ptr<TEvPrivate::TEvWriteIndex> txEvent)
            : TxEvent(std::move(txEvent))
        {}
    };

    struct TEvCompaction : public TEventLocal<TEvCompaction, EvIndexing> {
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GroupedBlobRanges;
        THashSet<TUnifiedBlobId> Externals;

        explicit TEvCompaction(std::unique_ptr<TEvPrivate::TEvWriteIndex> txEvent, IBlobExporter& blobManager)
            : TxEvent(std::move(txEvent))
        {
            TxEvent->GranuleCompaction = true;
            Y_VERIFY(TxEvent->IndexChanges);

            GroupedBlobRanges = NOlap::TColumnEngineChanges::GroupedBlobRanges(TxEvent->IndexChanges->SwitchedPortions);

            if (blobManager.HasExternBlobs()) {
                for (const auto& [blobId, _] : GroupedBlobRanges) {
                    TEvictMetadata meta;
                    if (blobManager.GetEvicted(blobId, meta).IsExternal()) {
                        Externals.insert(blobId);
                    }
                }
            }
        }
    };

    struct TEvEviction : public TEventLocal<TEvEviction, EvEviction> {
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GroupedBlobRanges;
        THashSet<TUnifiedBlobId> Externals;

        explicit TEvEviction(std::unique_ptr<TEvPrivate::TEvWriteIndex> txEvent, IBlobExporter& blobManager,
                             bool needWrites)
            : TxEvent(std::move(txEvent))
        {
            Y_VERIFY(TxEvent->IndexChanges);

            if (needWrites) {
                GroupedBlobRanges =
                    NOlap::TColumnEngineChanges::GroupedBlobRanges(TxEvent->IndexChanges->PortionsToEvict);

                if (blobManager.HasExternBlobs()) {
                    for (auto& [blobId, _] : GroupedBlobRanges) {
                        TEvictMetadata meta;
                        if (blobManager.GetEvicted(blobId, meta).IsExternal()) {
                            Externals.insert(blobId);
                        }
                    }
                }
            } else {
                TxEvent->SetPutStatus(NKikimrProto::OK);
            }
        }

        bool NeedDataReadWrite() const {
            return (TxEvent->GetPutStatus() != NKikimrProto::OK);
        }
    };

    struct TEvS3Settings : public TEventLocal<TEvS3Settings, EvS3Settings> {
        NKikimrSchemeOp::TS3Settings Settings;

        explicit TEvS3Settings(const NKikimrSchemeOp::TS3Settings& settings)
            : Settings(settings)
        {}
    };

    struct TEvExport : public TEventLocal<TEvExport, EvExport> {
        using TBlobDataMap = THashMap<TUnifiedBlobId, TString>;

        NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
        ui64 ExportNo = 0;
        TString TierName;
        TActorId DstActor;
        TBlobDataMap Blobs; // src: blobId -> data map; dst: exported blobIds set
        THashMap<TUnifiedBlobId, TUnifiedBlobId> SrcToDstBlobs;
        TMap<TString, TString> ErrorStrings;

        explicit TEvExport(ui64 exportNo, const TString& tierName, ui64 pathId,
                           const THashSet<TUnifiedBlobId>& blobIds)
            : ExportNo(exportNo)
            , TierName(tierName)
        {
            Y_VERIFY(ExportNo);
            Y_VERIFY(!TierName.empty());
            Y_VERIFY(pathId);
            Y_VERIFY(!blobIds.empty());

            for (auto& blobId : blobIds) {
                Blobs.emplace(blobId, TString());
                SrcToDstBlobs[blobId] = blobId.MakeS3BlobId(pathId);
            }
        }

        explicit TEvExport(ui64 exportNo, const TString& tierName, const THashSet<NOlap::TEvictedBlob>& evictSet)
            : ExportNo(exportNo)
            , TierName(tierName)
        {
            Y_VERIFY(ExportNo);
            Y_VERIFY(!TierName.empty());
            Y_VERIFY(!evictSet.empty());

            for (auto& evict : evictSet) {
                Y_VERIFY(evict.IsEvicting());
                Y_VERIFY(evict.ExternBlob.IsS3Blob());

                Blobs.emplace(evict.Blob, TString());
                SrcToDstBlobs[evict.Blob] = evict.ExternBlob;
            }
        }

        void AddResult(const TUnifiedBlobId& blobId, const TString& key, const bool hasError, const TString& errStr) {
            if (hasError) {
                Status = NKikimrProto::ERROR;
                Y_VERIFY(ErrorStrings.emplace(key, errStr).second, "%s", key.data());
                Blobs.erase(blobId);
            } else if (!ErrorStrings.contains(key)) { // (OK + !OK) == !OK
                Y_VERIFY(Blobs.contains(blobId));
                if (Status == NKikimrProto::UNKNOWN) {
                    Status = NKikimrProto::OK;
                }
            }
        }

        bool Finished() const {
            return (Blobs.size() + ErrorStrings.size()) == SrcToDstBlobs.size();
        }

        TString SerializeErrorsToString() const {
            TStringBuilder sb;
            for (auto&& i : ErrorStrings) {
                sb << i.first << "=" << i.second << ";";
            }
            return sb;
        }
    };

    struct TEvForget: public TEventLocal<TEvForget, EvForget> {
        NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
        std::vector<NOlap::TEvictedBlob> Evicted;
        TString ErrorStr;
    };

    struct TEvGetExported : public TEventLocal<TEvGetExported, EvGetExported> {
        TActorId DstActor; // It's a BlobCache actor. S3 actor sends TEvReadBlobRangesResult to it as result
        ui64 DstCookie;
        NOlap::TEvictedBlob Evicted;
        std::vector<NOlap::TBlobRange> BlobRanges;
    };

    struct TEvScanStats : public TEventLocal<TEvScanStats, EvScanStats> {
        TEvScanStats(ui64 rows, ui64 bytes) : Rows(rows), Bytes(bytes) {}
        ui64 Rows;
        ui64 Bytes;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        explicit TEvReadFinished(ui64 requestCookie, ui64 txId = 0)
            : RequestCookie(requestCookie), TxId(txId)
        {}

        ui64 RequestCookie;
        ui64 TxId;
    };

    struct TEvPeriodicWakeup : public TEventLocal<TEvPeriodicWakeup, EvPeriodicWakeup> {
        TEvPeriodicWakeup(bool manual = false)
            : Manual(manual)
        {}

        bool Manual;
    };

    class TEvWriteBlobsResult : public TEventLocal<TEvWriteBlobsResult, EvWriteBlobsResult> {
    public:
        class TPutBlobData {
            YDB_READONLY_DEF(TUnifiedBlobId, BlobId);
            YDB_READONLY_DEF(TString, BlobData);
            YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, ParsedBatch);
            YDB_ACCESSOR_DEF(TString, LogicalMeta);
        public:
            TPutBlobData() = default;

            TPutBlobData(const TUnifiedBlobId& blobId, const TString& data, const std::shared_ptr<arrow::RecordBatch>& batch)
                : BlobId(blobId)
                , BlobData(data)
                , ParsedBatch(batch)
            {}
        };

        TEvWriteBlobsResult(const NColumnShard::TBlobPutResult::TPtr& putResult, const NEvWrite::TWriteMeta& writeMeta, const NOlap::TSnapshot& snapshot)
            : PutResult(putResult)
            , WriteMeta(writeMeta)
            , Snapshot(snapshot)
        {
            Y_VERIFY(PutResult);
        }

        TEvWriteBlobsResult(const NColumnShard::TBlobPutResult::TPtr& putResult, TPutBlobData&& blobData, const NEvWrite::TWriteMeta& writeMeta, const NOlap::TSnapshot& snapshot)
            : TEvWriteBlobsResult(putResult, writeMeta, snapshot)
        {
            BlobData = std::move(blobData);
        }

        const TPutBlobData& GetBlobData() const {
            return BlobData;
        }

        const NColumnShard::TBlobPutResult& GetPutResult() const {
            return *PutResult;
        }

        const NColumnShard::TBlobPutResult::TPtr GetPutResultPtr() {
            return PutResult;
        }

        const NEvWrite::TWriteMeta& GetWriteMeta() const {
            return WriteMeta;
        }

        const NOlap::TSnapshot& GetSnapshot() const {
            return Snapshot;
        }

    private:
        NColumnShard::TBlobPutResult::TPtr PutResult;
        TPutBlobData BlobData;
        NEvWrite::TWriteMeta WriteMeta;
        NOlap::TSnapshot Snapshot;
    };
};

}
