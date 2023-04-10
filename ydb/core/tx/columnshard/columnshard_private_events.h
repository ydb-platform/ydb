#pragma once

#include "blob_manager.h"
#include "defs.h"

#include <ydb/core/protos/counters_columnshard.pb.h>

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
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    /// Common event for Indexing and GranuleCompaction: write index data in TTxWriteIndex transaction.
    struct TEvWriteIndex : public TEventLocal<TEvWriteIndex, EvWriteIndex> {
        NKikimrProto::EReplyStatus PutStatus = NKikimrProto::UNKNOWN;
        NOlap::TIndexInfo IndexInfo;
        std::shared_ptr<NOlap::TColumnEngineChanges> IndexChanges;
        THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CachedBlobs;
        TVector<TString> Blobs;
        bool GranuleCompaction{false};
        TBlobBatch BlobBatch;
        TUsage ResourceUsage;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        bool CacheData{false};
        TDuration Duration;

        TEvWriteIndex(NOlap::TIndexInfo&& indexInfo,
            std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges,
            bool cacheData,
            THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>>&& cachedBlobs = {})
            : IndexInfo(std::move(indexInfo))
            , IndexChanges(indexChanges)
            , CachedBlobs(std::move(cachedBlobs))
            , CacheData(cacheData)
        {}
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
                for (auto& [blobId, _] : GroupedBlobRanges) {
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
                TxEvent->PutStatus = NKikimrProto::OK;
            }
        }

        bool NeedDataReadWrite() const {
            return (TxEvent->PutStatus != NKikimrProto::OK);
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
        ui64 PathId = 0;
        TActorId DstActor;
        TBlobDataMap Blobs; // src: blobId -> data map; dst: exported blobIds set
        THashMap<TUnifiedBlobId, TUnifiedBlobId> SrcToDstBlobs;
        TMap<TString, TString> ErrorStrings;

        explicit TEvExport(ui64 exportNo, const TString& tierName, ui64 pathId, TBlobDataMap&& tierBlobs)
            : ExportNo(exportNo)
            , TierName(tierName)
            , PathId(pathId)
            , Blobs(std::move(tierBlobs))
        {
            Y_VERIFY(ExportNo);
            Y_VERIFY(!TierName.empty());
            Y_VERIFY(PathId);
            Y_VERIFY(!Blobs.empty());
        }

        TEvExport(ui64 exportNo, const TString& tierName, ui64 pathId, TActorId dstActor, TBlobDataMap&& blobs)
            : ExportNo(exportNo)
            , TierName(tierName)
            , PathId(pathId)
            , DstActor(dstActor)
            , Blobs(std::move(blobs))
        {
            Y_VERIFY(ExportNo);
            Y_VERIFY(!TierName.empty());
            Y_VERIFY(PathId);
            Y_VERIFY(DstActor);
            Y_VERIFY(!Blobs.empty());
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
};

}
