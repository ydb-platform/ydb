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

        enum {
            EvResume = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TExPoison {};

    public:
        TDefragQuantum(const std::shared_ptr<TDefragCtx>& dctx, const TVDiskID& selfVDiskId,
                std::optional<TChunksToDefrag> chunksToDefrag)
            : TActorCoroImpl(64_KB, true)
            , DCtx(dctx)
            , SelfVDiskId(selfVDiskId)
            , ChunksToDefrag(std::move(chunksToDefrag))
        {}

        void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                    throw TExPoison();
            }

            Y_ABORT("unexpected event Type# 0x%08" PRIx32, ev->GetTypeRewrite());
        }

        void Run() override {
            try {
                RunImpl();
            } catch (const TExPoison&) {
                return;
            }
        }

        void RunImpl() {
            TEvDefragQuantumResult::TStat stat{.Eof = true};

            if (ChunksToDefrag) {
                Y_ABORT_UNLESS(*ChunksToDefrag);
            } else {
                TDefragQuantumFindChunks findChunks(GetSnapshot(), DCtx->HugeBlobCtx);
                const ui64 endTime = GetCycleCountFast() + DurationToCycles(NDefrag::MaxSnapshotHoldDuration);
                while (findChunks.Scan(NDefrag::WorkQuantum)) {
                    if (GetCycleCountFast() >= endTime) {
                        return (void)Send(ParentActorId, new TEvDefragQuantumResult(std::move(stat)));
                    }
                    Yield();
                }
                ChunksToDefrag.emplace(findChunks.GetChunksToDefrag(DCtx->MaxChunksToDefrag));
            }
            if (*ChunksToDefrag) {
                stat.FoundChunksToDefrag = ChunksToDefrag->FoundChunksToDefrag;
                stat.FreedChunks = ChunksToDefrag->Chunks;
                stat.Eof = stat.FoundChunksToDefrag < DCtx->MaxChunksToDefrag;

                auto lockedChunks = LockChunks(*ChunksToDefrag);

                TDefragQuantumFindRecords findRecords(std::move(*ChunksToDefrag), DCtx->VCtx->Top->GType);
                while (findRecords.Scan(NDefrag::WorkQuantum, GetSnapshot())) {
                    Yield();
                }

                const TActorId rewriterActorId = Register(CreateDefragRewriter(DCtx, SelfVDiskId, SelfActorId,
                    findRecords.GetRecordsToRewrite()));
                THolder<TEvDefragRewritten::THandle> ev;
                try {
                    ev = WaitForSpecificEvent<TEvDefragRewritten>(&TDefragQuantum::ProcessUnexpectedEvent);
                } catch (const TExPoison&) {
                    Send(new IEventHandle(TEvents::TSystem::Poison, 0, rewriterActorId, {}, nullptr, 0));
                    throw;
                }
                stat.RewrittenRecs = ev->Get()->RewrittenRecs;
                stat.RewrittenBytes = ev->Get()->RewrittenBytes;

                Compact();

                auto hugeStat = GetHugeStat();
                Y_DEBUG_ABORT_UNLESS(hugeStat.LockedChunks.size() < 100);
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

        void Compact() {
            Send(DCtx->SkeletonId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
            WaitForSpecificEvent<TEvCompactVDiskResult>(&TDefragQuantum::ProcessUnexpectedEvent);
        }

        NHuge::THeapStat GetHugeStat() {
            Send(DCtx->HugeKeeperId, new TEvHugeStat());
            return std::move(WaitForSpecificEvent<TEvHugeStatResult>(&TDefragQuantum::ProcessUnexpectedEvent)->Get()->Stat);
        }
    };

    IActor *CreateDefragQuantumActor(const std::shared_ptr<TDefragCtx>& dctx, const TVDiskID& selfVDiskId,
            std::optional<TChunksToDefrag> chunksToDefrag) {
        return new TActorCoro(MakeHolder<TDefragQuantum>(dctx, selfVDiskId, std::move(chunksToDefrag)),
            NKikimrServices::TActivity::BS_DEFRAG_QUANTUM);
    }

} // NKikimr
