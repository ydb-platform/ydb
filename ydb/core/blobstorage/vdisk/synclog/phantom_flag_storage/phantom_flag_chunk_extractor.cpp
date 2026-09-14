#include "phantom_flag_chunk_extractor.h"
#include "phantom_flags.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmem.h>

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////
// TPhantomFlagChunkExtractorActor
//
// One actor instance per pending source chunk. Reads the chunk's used
// pages from the PDisk, walks every TSyncLogPage via TSyncLogPageROIterator,
// and accumulates DoNotKeep TLogoBlobRec records that are still behind
// the per-tablet/channel threshold on at least one unsynced disk
// (matching the inline path in TPhantomFlagStorageState::PutOne).
// Sends the batch to the Processor, which atomically persists them and
// retires the source chunk via TEvPhantomFlagStorageCommitData.RetiredChunks.
////////////////////////////////////////////////////////////////////////////
class TPhantomFlagChunkExtractorActor : public TActorBootstrapped<TPhantomFlagChunkExtractorActor> {
public:
    TPhantomFlagChunkExtractorActor(const TIntrusivePtr<TSyncLogCtx>& slCtx,
            const TActorId& processorId, TDeletedChunk chunk, ui32 appendBlockSize,
            std::shared_ptr<const TPhantomFlagThresholds>&& thresholds, TSyncedMask syncedMask)
        : TActorBootstrapped<TPhantomFlagChunkExtractorActor>()
        , SlCtx(slCtx)
        , ProcessorId(processorId)
        , Chunk(chunk)
        , AppendBlockSize(appendBlockSize)
        , Thresholds(std::move(thresholds))
        , SyncedMask(syncedMask)
    {}

    void Bootstrap() {
        Send(SlCtx->PDiskCtx->PDiskId, new NPDisk::TEvChunkRead(
                SlCtx->PDiskCtx->Dsk->Owner, SlCtx->PDiskCtx->Dsk->OwnerRound,
                Chunk.ChunkIdx, /*offset=*/0, Chunk.UsedPagesNum * AppendBlockSize,
                NPriRead::SyncLog, nullptr));
        Become(&TThis::StateFunc);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PHANTOM_FLAG_CHUNK_EXTRACTOR;
    }

private:
    void Handle(const NPDisk::TEvChunkReadResult::TPtr& ev) {
        const ui32 size = Chunk.UsedPagesNum * AppendBlockSize;
        ++SlCtx->PhantomFlagStorageGroup.BuilderReadsFromDisk();
        SlCtx->PhantomFlagStorageGroup.BuilderReadsFromDiskBytes() += size;

        const NPDisk::TEvChunkReadResult* msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            const TBufferWithGaps& readData = msg->Data;
            for (ui32 pageIdx = 0; pageIdx < Chunk.UsedPagesNum; pageIdx++) {
                const ui32 offset = pageIdx * AppendBlockSize;
                if (readData.IsReadable(offset, AppendBlockSize)) {
                    const TSyncLogPage* page =
                            readData.DataPtr<const TSyncLogPage>(offset, AppendBlockSize);
                    TSyncLogPageROIterator it(page);
                    for (it.SeekToFirst(); it.Valid(); it.Next()) {
                        ProcessRecord(it.Get());
                    }
                }
            }
        }
        Finish();
    }

    void ProcessRecord(const TRecordHdr* hdr) {
        if (hdr->RecType != TRecordHdr::RecLogoBlob) {
            return;
        }
        const TLogoBlobRec* blob = hdr->GetLogoBlob();
        if (!blob->Ingress.IsDoNotKeep(SlCtx->VCtx->Top->GType)) {
            return;
        }
        if (!Thresholds || Thresholds->IsBehindThresholdOnUnsynced(blob->LogoBlobID(), SyncedMask)) {
            Flags.push_back(*blob);
        }
    }

    void Finish() {
        Send(ProcessorId, new TEvPhantomFlagExtractedFromChunk(Chunk.ChunkIdx, std::move(Flags)));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NPDisk::TEvChunkReadResult, Handle)
    )

private:
    TIntrusivePtr<TSyncLogCtx> SlCtx;
    const TActorId ProcessorId;
    const TDeletedChunk Chunk;
    const ui32 AppendBlockSize;
    const std::shared_ptr<const TPhantomFlagThresholds> Thresholds;
    const TSyncedMask SyncedMask;
    TPhantomFlags Flags;
};


NActors::IActor* CreatePhantomFlagChunkExtractorActor(
        const TIntrusivePtr<TSyncLogCtx>& slCtx,
        const NActors::TActorId& processorId,
        TDeletedChunk chunk,
        ui32 appendBlockSize,
        std::shared_ptr<const TPhantomFlagThresholds> thresholds,
        TSyncedMask syncedMask) {
    return new TPhantomFlagChunkExtractorActor(slCtx, processorId, chunk,
            appendBlockSize, std::move(thresholds), syncedMask);
}

} // namespace NSyncLog

} // namespace NKikimr
