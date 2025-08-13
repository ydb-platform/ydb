#include "phantom_flag_storage_builder.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
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
            const TActorId& keeperId, const TSyncLogSnapshotPtr& snapshot)
        : TActorBootstrapped<TPhantomFlagStorageBuilderActor>()
        , SlCtx(slCtx)
        , KeeperId(keeperId)
        , Snapshot(snapshot)
    {}

    void Bootstrap(const TActorContext&) {
        LastLsn = Snapshot->LogStartLsn;
        DiskIt = TDiskRecLogSnapshot::TIndexRecIterator(Snapshot->DiskSnapPtr);
        DiskIt.Seek(LastLsn);
        ReadNextChunk();
        Become(&TThis::StateFunc);
    }

private:
    void Handle(const NPDisk::TEvChunkReadResult::TPtr& ev) {
        auto [chunkIdx, idxRec] = DiskIt.Get();
        const NPDisk::TEvChunkReadResult* msg = ev->Get();
        const TBufferWithGaps& readData = ev->Get()->Data;

        Y_VERIFY_S(chunkIdx == msg->ChunkIdx &&
                idxRec->OffsetInPages * Snapshot->AppendBlockSize == msg->Offset &&
                idxRec->PagesNum * Snapshot->AppendBlockSize == readData.Size(),
            SlCtx->VCtx->VDiskLogPrefix
            << "Phantom Flag Storage Builder failed to read synclog: chunkIdx# " << chunkIdx
            << " msgChunkIdx# " << msg->ChunkIdx << " OffsetInPages# " << idxRec->OffsetInPages
            << " appendBlockSize# " << Snapshot->AppendBlockSize << " msgOffset# " << msg->Offset
            << " PagesNum# " << idxRec->PagesNum << " readDataSize# " << ui32(readData.Size()));

        // process all pages
        for (ui32 pageIdx = 0; pageIdx < idxRec->PagesNum; pageIdx++) {
            const TSyncLogPage *page = readData.DataPtr<const TSyncLogPage>(
                    pageIdx * Snapshot->AppendBlockSize, Snapshot->AppendBlockSize);

            // process one page
            TSyncLogPageROIterator it(page);
            for (it.SeekToFirst(); it.Valid(); it.Next()) {
                ProcessRecord(it.Get());
            }
        }

        DiskIt.Next();
        ReadNextChunk();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PHANTOM_FLAG_STORAGE_BUILDER;
    }

    void ProcessMemSnapshot() {
        TMemRecLogSnapshot::TIterator it(Snapshot->MemSnapPtr);
        for (it.Seek(Snapshot->LogStartLsn); it.Valid(); it.Next()) {
            ProcessRecord(it.Get());
        }
    }

    void ProcessRecord(const TRecordHdr* hdr) {
        if (LastLsn < hdr->Lsn && hdr->RecType == TRecordHdr::RecLogoBlob) {
            const TLogoBlobRec* blob = hdr->GetLogoBlob();
            TIngress::EMode ingressMode = TIngress::IngressMode(SlCtx->VCtx->Top->GType);
            if (blob->Ingress.GetCollectMode(ingressMode) == ECollectMode::CollectModeDoNotKeep) {
                AddFlag(*blob);
            }
            LastLsn = hdr->Lsn;
        }
    }

    void ReadNextChunk() {
        if (DiskIt.Valid()) {
            auto [chunkIdx, idxRec] = DiskIt.Get();

            const ui32 offset = idxRec->OffsetInPages * Snapshot->AppendBlockSize;
            const ui32 size = idxRec->PagesNum * Snapshot->AppendBlockSize;

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
        Send(KeeperId, new TEvPhantomFlagStorageAddFlagsFromSnapshot(std::move(Flags)));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NPDisk::TEvChunkReadResult, Handle)
    )

private:
    TIntrusivePtr<TSyncLogCtx> SlCtx;
    const TActorId KeeperId;
    TSyncLogSnapshotPtr Snapshot;
    TDiskRecLogSnapshot::TIndexRecIterator DiskIt;
    TPhantomFlags Flags;
    ui64 LastLsn;
};


NActors::IActor* CreatePhantomFlagStorageBuilderActor(const TIntrusivePtr<TSyncLogCtx>& slCtx,
    const TActorId& keeperId, const TSyncLogSnapshotPtr& snapshot) {
    return new TPhantomFlagStorageBuilderActor(slCtx, keeperId, snapshot);
}

} // namespace NSyncLog

} // namespace NKikimr
