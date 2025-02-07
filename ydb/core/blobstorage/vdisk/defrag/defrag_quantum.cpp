#include "defrag_quantum.h"
#include "defrag_search.h"
#include "defrag_rewriter.h"
#include <ydb/core/blobstorage/vdisk/common/sublog.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/actor_coroutine.h>

using namespace NKikimrServices;

namespace NKikimr {

    class TDefragQuantum : public TActorCoroImpl {
        std::shared_ptr<TDefragCtx> DCtx;
        const TVDiskID SelfVDiskId;
        std::optional<TChunksToDefrag> ChunksToDefrag;
        const ui32 MinHugeBlobInBytes;

        enum {
            EvResume = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TExPoison {};

    public:
        TDefragQuantum(const std::shared_ptr<TDefragCtx>& dctx, const TVDiskID& selfVDiskId,
                std::optional<TChunksToDefrag> chunksToDefrag, ui32 minHugeBlobInBytes)
            : TActorCoroImpl(64_KB, true)
            , DCtx(dctx)
            , SelfVDiskId(selfVDiskId)
            , ChunksToDefrag(std::move(chunksToDefrag))
            , MinHugeBlobInBytes(minHugeBlobInBytes)
        {}

        void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                    throw TExPoison();
            }

            Y_ABORT("unexpected event Type# 0x%08" PRIx32, ev->GetTypeRewrite());
        }

        void Run() override {
            STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD00, DCtx->VCtx->VDiskLogPrefix << "defrag quantum start",
                (ActorId, SelfActorId));

            try {
                RunImpl();
            } catch (const TExPoison&) {
            }

            STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD06, DCtx->VCtx->VDiskLogPrefix << "defrag quantum end",
                (ActorId, SelfActorId));
        }

        void RunImpl() {
            TEvDefragQuantumResult::TStat stat{.Eof = true};

            if (!ChunksToDefrag) {
                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD07, DCtx->VCtx->VDiskLogPrefix << "going to find chunks to defrag",
                    (ActorId, SelfActorId));

                TDefragQuantumFindChunks findChunks(GetSnapshot(), DCtx->HugeBlobCtx, ChunksToDefrag ?
                    std::make_optional(std::move(ChunksToDefrag->ChunksToShred)) : std::nullopt);
                const ui64 endTime = GetCycleCountFast() + DurationToCycles(NDefrag::MaxSnapshotHoldDuration);
                while (findChunks.Scan(NDefrag::WorkQuantum)) {
                    if (GetCycleCountFast() >= endTime) {
                        STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD08, DCtx->VCtx->VDiskLogPrefix
                            << "timed out while finding chunks to defrag", (ActorId, SelfActorId),
                            (Stat, stat));
                        return (void)Send(ParentActorId, new TEvDefragQuantumResult(std::move(stat)));
                    }
                    Yield();
                }
                ChunksToDefrag.emplace(findChunks.GetChunksToDefrag(DCtx->MaxChunksToDefrag));
            } else {
                Y_ABORT_UNLESS(*ChunksToDefrag || ChunksToDefrag->IsShred());
            }
            if (*ChunksToDefrag || ChunksToDefrag->IsShred()) {
                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD09, DCtx->VCtx->VDiskLogPrefix
                    << "commencing defragmentation", (ActorId, SelfActorId), (ChunksToDefrag, *ChunksToDefrag));

                if (!ChunksToDefrag->IsShred()) {
                    stat.FoundChunksToDefrag = ChunksToDefrag->FoundChunksToDefrag;
                    stat.Eof = stat.FoundChunksToDefrag < DCtx->MaxChunksToDefrag;

                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD10, DCtx->VCtx->VDiskLogPrefix << "locking chunks",
                        (ActorId, SelfActorId));

                    auto lockedChunks = LockChunks(*ChunksToDefrag);
                    ChunksToDefrag->Chunks = std::move(lockedChunks);
                    stat.FreedChunks = ChunksToDefrag->Chunks;

                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD11, DCtx->VCtx->VDiskLogPrefix << "locked chunks",
                        (ActorId, SelfActorId), (LockedChunks, ChunksToDefrag->Chunks));
                }

                TDefragQuantumFindRecords findRecords(std::move(*ChunksToDefrag), DCtx->VCtx->Top->GType,
                    DCtx->AddHeader, DCtx->HugeBlobCtx, MinHugeBlobInBytes);
                while (findRecords.Scan(NDefrag::WorkQuantum, GetSnapshot())) {
                    Yield();
                }

                if (auto records = findRecords.GetRecordsToRewrite(); !records.empty()) {
                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD12, DCtx->VCtx->VDiskLogPrefix << "rewriting records",
                        (ActorId, SelfActorId), (NumRecordsToRewrite, records.size()));

                    const TActorId rewriterActorId = Register(CreateDefragRewriter(DCtx, SelfVDiskId, SelfActorId,
                        std::move(records)));
                    THolder<TEvDefragRewritten::THandle> ev;
                    try {
                        ev = WaitForSpecificEvent<TEvDefragRewritten>(&TDefragQuantum::ProcessUnexpectedEvent);
                    } catch (const TExPoison&) {
                        Send(new IEventHandle(TEvents::TSystem::Poison, 0, rewriterActorId, {}, nullptr, 0));
                        throw;
                    }
                    stat.RewrittenRecs = ev->Get()->RewrittenRecs;
                    stat.RewrittenBytes = ev->Get()->RewrittenBytes;
                } else if (ChunksToDefrag->IsShred()) {
                    // no records to rewrite found
                    stat.Eof = true;
                }

                auto tablesToCompact = std::move(findRecords.GetTablesToCompact());

                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD13, DCtx->VCtx->VDiskLogPrefix << "compacting",
                    (ActorId, SelfActorId), (TablesToCompact, tablesToCompact));

                Compact(tablesToCompact);
            }

            Send(ParentActorId, new TEvDefragQuantumResult(std::move(stat)));
        }

        THullDsSnap GetSnapshot() {
            Send(DCtx->SkeletonId, new TEvTakeHullSnapshot(false));
            return std::move(WaitForSpecificEvent<TEvTakeHullSnapshotResult>(&TDefragQuantum::ProcessUnexpectedEvent)->Get()->Snap);
        }

        void Yield() {
            Send(new IEventHandle(EvResume, 0, SelfActorId, {}, nullptr, 0));
            WaitForSpecificEvent([](IEventHandle& ev) { return ev.Type == EvResume; }, &TDefragQuantum::ProcessUnexpectedEvent);
        }

        TDefragChunks LockChunks(const TChunksToDefrag& chunks) {
            Send(DCtx->HugeKeeperId, new TEvHugeLockChunks(chunks.Chunks));
            auto res = WaitForSpecificEvent<TEvHugeLockChunksResult>(&TDefragQuantum::ProcessUnexpectedEvent);
            return res->Get()->LockedChunks;
        }

        void Compact(THashSet<ui64> tablesToCompact) {
            Send(DCtx->SkeletonId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, tablesToCompact));
            WaitForSpecificEvent<TEvCompactVDiskResult>(&TDefragQuantum::ProcessUnexpectedEvent);
        }
    };

    IActor *CreateDefragQuantumActor(const std::shared_ptr<TDefragCtx>& dctx, const TVDiskID& selfVDiskId,
            std::optional<TChunksToDefrag> chunksToDefrag, ui32 minHugeBlobInBytes) {
        return new TActorCoro(MakeHolder<TDefragQuantum>(dctx, selfVDiskId, std::move(chunksToDefrag), minHugeBlobInBytes),
            NKikimrServices::TActivity::BS_DEFRAG_QUANTUM);
    }

} // NKikimr

template<>
void Out<NKikimr::TEvDefragQuantumResult::TStat>(IOutputStream& s, const NKikimr::TEvDefragQuantumResult::TStat& x) {
    x.Output(s);
}
