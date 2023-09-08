#pragma once

#include "blob_manager.h"
#include "defs.h"

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/ev_write/write_data.h>
#include <ydb/core/formats/arrow/special_keys.h>

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
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    /// Common event for Indexing and GranuleCompaction: write index data in TTxWriteIndex transaction.
    struct TEvWriteIndex : public TEventLocal<TEvWriteIndex, EvWriteIndex> {
        NOlap::TVersionedIndex IndexInfo;
        std::shared_ptr<NOlap::TColumnEngineChanges> IndexChanges;
        bool GranuleCompaction{false};
        TBlobBatch BlobBatch;
        TUsage ResourceUsage;
        bool CacheData{false};
        TDuration Duration;
        TBlobPutResult::TPtr PutResult;

        TEvWriteIndex(NOlap::TVersionedIndex&& indexInfo,
            std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges,
            bool cacheData)
            : IndexInfo(std::move(indexInfo))
            , IndexChanges(indexChanges)
            , CacheData(cacheData)
        {
            PutResult = std::make_shared<TBlobPutResult>(NKikimrProto::UNKNOWN);
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
            YDB_READONLY_DEF(TBlobRange, BlobRange);
            YDB_READONLY_DEF(TString, BlobData);
            YDB_READONLY_DEF(NKikimrTxColumnShard::TLogicalMetadata, LogicalMeta);
            YDB_ACCESSOR(ui64, RowsCount, 0);
            YDB_ACCESSOR(ui64, RawBytes, 0);
        public:
            TPutBlobData() = default;

            TPutBlobData(const TBlobRange& blobRange, const TString& data, const NArrow::TFirstLastSpecialKeys& specialKeys, ui64 rowsCount, ui64 rawBytes, const TInstant dirtyTime)
                : BlobRange(blobRange)
                , BlobData(data)
                , RowsCount(rowsCount)
                , RawBytes(rawBytes)
            {
                LogicalMeta.SetNumRows(rowsCount);
                LogicalMeta.SetRawBytes(rawBytes);
                LogicalMeta.SetDirtyWriteTimeSeconds(dirtyTime.Seconds());
                LogicalMeta.SetSpecialKeysRawData(specialKeys.SerializeToString());
            }
        };

        TEvWriteBlobsResult(const NColumnShard::TBlobPutResult::TPtr& putResult, const NEvWrite::TWriteMeta& writeMeta)
            : PutResult(putResult)
            , WriteMeta(writeMeta)
        {
            Y_VERIFY(PutResult);
        }

        TEvWriteBlobsResult(const NColumnShard::TBlobPutResult::TPtr& putResult, TVector<TPutBlobData>&& blobData, const NEvWrite::TWriteMeta& writeMeta, const ui64 schemaVersion)
            : TEvWriteBlobsResult(putResult, writeMeta)
        {
            BlobData = std::move(blobData);
            SchemaVersion = schemaVersion;
        }

        const TVector<TPutBlobData>& GetBlobData() const {
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

        ui64 GetSchemaVersion() const {
            return SchemaVersion;
        }

    private:
        NColumnShard::TBlobPutResult::TPtr PutResult;
        TVector<TPutBlobData> BlobData;
        NEvWrite::TWriteMeta WriteMeta;
        ui64 SchemaVersion = 0;
    };
};

}
