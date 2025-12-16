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
            const TActorId& keeperId, TSyncLogSnapshotPtr snapshot)
        : TActorBootstrapped<TPhantomFlagStorageBuilderActor>()
        , SlCtx(slCtx)
        , KeeperId(keeperId)
        , SyncLogSnapshot(snapshot)
        , Thresholds(slCtx->VCtx->Top->GType)
    {}

    void Bootstrap() {
        LastLsn = SyncLogSnapshot->LogStartLsn;
        DiskIt = TDiskRecLogSnapshot::TIndexRecIterator(SyncLogSnapshot->DiskSnapPtr);
        DiskIt.Seek(LastLsn);
        Send(SlCtx->SkeletonId, new TEvTakeHullSnapshot(false));
        Become(&TThis::StateWaitHullSnapshot);
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

    void Finish() {
        Send(KeeperId, new TEvPhantomFlagStorageFinishBuilder(std::move(Flags), std::move(Thresholds)));
        PassAway();
    }

    STRICT_STFUNC(StateWaitHullSnapshot,
        hFunc(TEvTakeHullSnapshotResult, Handle)
    )

    STRICT_STFUNC(StateFunc,
        hFunc(NPDisk::TEvChunkReadResult, Handle)
    )

private:
    TIntrusivePtr<TSyncLogCtx> SlCtx;
    const TActorId KeeperId;
    TSyncLogSnapshotPtr SyncLogSnapshot;
    TDiskRecLogSnapshot::TIndexRecIterator DiskIt;
    TPhantomFlags Flags;
    TPhantomFlagThresholds Thresholds;
    ui64 LastLsn;
};


NActors::IActor* CreatePhantomFlagStorageBuilderActor(const TIntrusivePtr<TSyncLogCtx>& slCtx,
    const TActorId& keeperId, TSyncLogSnapshotPtr snapshot) {
    return new TPhantomFlagStorageBuilderActor(slCtx, keeperId, snapshot);
}

} // namespace NSyncLog

} // namespace NKikimr
