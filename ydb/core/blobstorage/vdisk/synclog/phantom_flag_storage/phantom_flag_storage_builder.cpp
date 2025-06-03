#include "phantom_flag_storage_builder.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_context.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////
// TPhantomFlagStorageBuilderActor
////////////////////////////////////////////////////////////////////////////
class TPhantomFlagStorageBuilderActor : public TActorBootstrapped<TPhantomFlagStorageBuilderActor> {
public:
    TPhantomFlagStorageBuilderActor(TSyncLogSnapshotPtr snapshot)
        : TActorBootstrapped<TPhantomFlagStorageBuilderActor>()
        , Snapshot(snapshot)
    {}

    void Bootstrap(const TActorContext &ctx) {
        LastLsn = Snapshot->LogStartLsn;
        DiskIt = TDiskRecLogSnapshot::TIndexRecIterator(SnapPtr->DiskSnapPtr);
        DiskIt.Seek(LastLsn);
        ProcessSyncLogMemSnapshot();
        ReadNextChunk();
        Become(&TThis::StateFunc);
    }

private:
    void Handle(const NPDisk::TEvChunkReadResult::TPtr& ev) {
        auto [chunkIdx, idxRec] = DiskIt.Get();
        const NPDisk::TEvChunkReadResult* msg = ev->Get();
        const TBufferWithGaps& readData = ev->Get()->Data;

        Y_VERIFY_S(chunkIdx == msg->ChunkIdx &&
                idxRec->OffsetInPages * SnapPtr->AppendBlockSize == msg->Offset &&
                idxRec->PagesNum * SnapPtr->AppendBlockSize == readData.Size(),
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
            it.SeekToFirst();
            for (it.SeekToFirst(); hi.Valid(); it.Next()) {
                ProcessRecord(it.Get());
            }
        }

        DiskIt.Next();
        ReadNextChunk();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PHANTOM_FLAG_STORAGE_BUILDER;
    }

    void TPhantomFlagStorage::ProcessMemSnapshot(TMemRecLogSnapshotConstPtr memSnapshot) {
        TMemRecLogSnapshot::TIterator it(Snapshot->MemSnapPtr);
        for (it.Seek(Snapshot->LogStartLsn); it.Valid(); it.Next()) {
            ProcessRecord(it.Get());
        }
    }

    bool TPhantomFlagStorage::ProcessRecord(const TRecordHdr* hdr) {
        if (LastLsn < hdr->Lsn && hdr->RecType == TRecordHdr::RecLogoBlob) {
            const TLogoBlobRec* blob = hdr->GetLogoBlob();
            if (blob->Ingress.GetCollectMode() == ECollectMode::CollectModeDoNotKeep) {
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
            ProcessMemSnapshots();
        }
    }

    void TPhantomFlagStorage::AddFlag(const TLogoBlobRec& flag) {
        Flags.push_back(flag);
    }

    void Finish() {
        Send(SlCtx->KeeperId, new TEvPhantomFlagStorageAddFlagsFromSnapshot(std::move(Flags)));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NPDisk::TEvChunkReadResult, Handle)
    )

private:
    TIntrusivePtr<TSyncLogCtx> SlCtx;
    TSyncLogSnapshotPtr Snapshot;
    TDiskRecLogSnapshot::TIndexRecIterator DiskIt;
    TPhantomFlags Flags;
    ui64 LastLsn;
};


NActors::IActor* CreatePhantomFlagBuilderActor(TIntrusivePtr<TSyncLogCtx> slCtx,
        TSyncLogSnapshotPtr snapshot) {
    return new TPhantomFlagStorageBuilderActor(slCtx, snapshot);
}

} // namespace NSyncLog

} // namespace NKikimr
