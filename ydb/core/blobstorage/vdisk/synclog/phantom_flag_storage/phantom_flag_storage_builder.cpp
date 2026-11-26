#include "phantom_flag_storage_builder.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_context.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////
// TPhantomFlagStorageBuilderActor
////////////////////////////////////////////////////////////////////////////
class TPhantomFlagStorageBuilderActor : public TActorBootstrapped<TPhantomFlagStorageBuilderActor> {
public:
    TPhantomFlagStorageBuilderActor(const TIntrusivePtr<TSyncLogCtx>& slCtx,
            const TActorId& requesterId, TSyncLogSnapshotPtr snapshot, bool buildThresholds,
            std::optional<TPhantomFlagStorageSnapshot> storageSnapshot = std::nullopt)
        : TActorBootstrapped<TPhantomFlagStorageBuilderActor>()
        , SlCtx(slCtx)
        , RequesterId(requesterId)
        , SyncLogSnapshot(snapshot)
        , Thresholds(slCtx->VCtx->Top->GType)
        , BuildThresholds(buildThresholds)
        , StorageSnapshot(std::move(storageSnapshot))
    {
        if (StorageSnapshot) {
            Flags = std::move(StorageSnapshot->Flags);
        }
    }

    void Bootstrap() {
        if (!SyncLogSnapshot) {
            Finish();
            return;
        }

        if (SyncLogSnapshot->LogStartLsn > 0) {
            LastLsn = SyncLogSnapshot->LogStartLsn - 1;
        }
        DiskIt = TDiskRecLogSnapshot::TIndexRecIterator(SyncLogSnapshot->DiskSnapPtr);
        DiskIt.Seek(LastLsn);
        if (BuildThresholds) {
            Send(SlCtx->SkeletonId, new TEvTakeHullSnapshot(false));
            Become(&TThis::StateWaitHullSnapshot);
        } else {
            Become(&TThis::StateFunc);
            ReadNextChunk();
        }
    }

private:
    void Handle(TEvTakeHullSnapshotResult::TPtr ev) {
        using TIndexForwardIterator = TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>::TIndexForwardIterator;
        TIndexForwardIterator it(ev->Get()->Snap.HullCtx, &ev->Get()->Snap.LogoBlobsSnap);
        for (it.Seek(TLogoBlobID(0, 0, 0)); it.Valid(); it.Next()) {
            if (it.GetMemRec().GetIngress().IsKeep(SlCtx->VCtx->Top->GType)) {
                Thresholds.AddBlob(it.GetCurKey().LogoBlobID());
            }
        }
        // to release snapshots
        ev.Reset();

        Become(&TThis::StateFunc);
        ReadNextChunk();
    }

    void Handle(const NPDisk::TEvChunkReadResult::TPtr& ev) {
        if (StorageSnapshot && (ev->Get()->Status != NKikimrProto::OK || !ev->Get()->Data.IsReadable())) {
            FinishSnapshot(/*readError=*/true);
            return;
        }
        if (ev->Get()->Status == NKikimrProto::OK) {
            auto [chunkIdx, idxRec] = DiskIt.Get();
            const NPDisk::TEvChunkReadResult* msg = ev->Get();
            const TBufferWithGaps& readData = ev->Get()->Data;

            Y_VERIFY_S(chunkIdx == msg->ChunkIdx &&
                    idxRec->OffsetInPages * SyncLogSnapshot->AppendBlockSize == msg->Offset &&
                    idxRec->PagesNum * SyncLogSnapshot->AppendBlockSize == readData.Size(),
                SlCtx->VCtx->VDiskLogPrefix
                << "Phantom Flag Storage Builder failed to read synclog: chunkIdx# " << chunkIdx
                << " msgChunkIdx# " << msg->ChunkIdx << " OffsetInPages# " << idxRec->OffsetInPages
                << " appendBlockSize# " << SyncLogSnapshot->AppendBlockSize << " msgOffset# " << msg->Offset
                << " PagesNum# " << idxRec->PagesNum << " readDataSize# " << ui32(readData.Size()));

            // process all pages
            for (ui32 pageIdx = 0; pageIdx < idxRec->PagesNum; pageIdx++) {
                ui32 offset = pageIdx * SyncLogSnapshot->AppendBlockSize;
                ui32 len = SyncLogSnapshot->AppendBlockSize;
                if (readData.IsReadable(offset, len)) {
                    const TSyncLogPage *page = readData.DataPtr<const TSyncLogPage>(offset, len);

                    // process one page
                    TSyncLogPageROIterator it(page);
                    for (it.SeekToFirst(); it.Valid(); it.Next()) {
                        ProcessRecord(it.Get());
                    }
                }
            }
        }

        DiskIt.Next();
        ReadNextChunk();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PHANTOM_FLAG_STORAGE_BUILDER;
    }

    void ProcessMemSnapshot() {
        TMemRecLogSnapshot::TIterator it(SyncLogSnapshot->MemSnapPtr);
        for (it.Seek(SyncLogSnapshot->LogStartLsn); it.Valid(); it.Next()) {
            ProcessRecord(it.Get());
        }
    }

    void ProcessRecord(const TRecordHdr* hdr) {
        if (LastLsn < hdr->Lsn && hdr->RecType == TRecordHdr::RecLogoBlob) {
            const TLogoBlobRec* blob = hdr->GetLogoBlob();
            if (blob->Ingress.IsDoNotKeep(SlCtx->VCtx->Top->GType)) {
                AddFlag(*blob);
            }
            LastLsn = hdr->Lsn;
        }
    }

    void ReadNextChunk() {
        if (DiskIt.Valid()) {
            auto [chunkIdx, idxRec] = DiskIt.Get();

            const ui32 offset = idxRec->OffsetInPages * SyncLogSnapshot->AppendBlockSize;
            const ui32 size = idxRec->PagesNum * SyncLogSnapshot->AppendBlockSize;

            // update mon counters
            ++SlCtx->PhantomFlagStorageGroup.BuilderReadsFromDisk();
            SlCtx->PhantomFlagStorageGroup.BuilderReadsFromDiskBytes() += size;

            // send read request
            Send(SlCtx->PDiskCtx->PDiskId, new NPDisk::TEvChunkRead(SlCtx->PDiskCtx->Dsk->Owner,
                    SlCtx->PDiskCtx->Dsk->OwnerRound, chunkIdx, offset, size, NPriRead::SyncLog, nullptr));
        } else {
            ProcessMemSnapshot();
            Finish();
        }
    }

    void AddFlag(const TLogoBlobRec& flag) {
        Flags.push_back(flag);
    }

    void FinishSnapshot(bool readError) {
        auto result = std::make_unique<TEvPhantomFlagStorageGetSnapshotResult>(
            std::move(Flags), std::move(StorageSnapshot->Thresholds), std::unordered_set<ui32>{}, /*eof=*/true);
        result->ReadError = readError;
        Send(RequesterId, result.release());
        PassAway();
    }

    void Finish() {
        if (StorageSnapshot) {
            FinishSnapshot(/*readError=*/false);
        } else {
            Send(RequesterId, new TEvPhantomFlagStorageFinishBuilder(std::move(Flags), std::move(Thresholds)));
            PassAway();
        }
    }

    STRICT_STFUNC(StateWaitHullSnapshot,
        hFunc(TEvTakeHullSnapshotResult, Handle)
    )

    STRICT_STFUNC(StateFunc,
        hFunc(NPDisk::TEvChunkReadResult, Handle)
    )

private:
    TIntrusivePtr<TSyncLogCtx> SlCtx;
    const TActorId RequesterId;
    TSyncLogSnapshotPtr SyncLogSnapshot;
    TDiskRecLogSnapshot::TIndexRecIterator DiskIt;
    TPhantomFlags Flags;
    TPhantomFlagThresholds Thresholds;
    ui64 LastLsn = 0;

    const bool BuildThresholds = true;
    std::optional<TPhantomFlagStorageSnapshot> StorageSnapshot;
};


NActors::IActor* CreatePhantomFlagStorageBuilderActor(const TIntrusivePtr<TSyncLogCtx>& slCtx,
        const TActorId& requesterId, TSyncLogSnapshotPtr snapshot, bool buildThresholds) {
    return new TPhantomFlagStorageBuilderActor(slCtx, requesterId, snapshot, buildThresholds);
}

NActors::IActor* CreatePhantomFlagStorageSnapshotActor(const TIntrusivePtr<TSyncLogCtx>& slCtx,
        const TActorId& requesterId, TSyncLogSnapshotPtr snapshot, TPhantomFlagStorageSnapshot storageSnapshot) {
    return new TPhantomFlagStorageBuilderActor(slCtx, requesterId, std::move(snapshot), false,
            std::move(storageSnapshot));
}

} // namespace NSyncLog

} // namespace NKikimr
