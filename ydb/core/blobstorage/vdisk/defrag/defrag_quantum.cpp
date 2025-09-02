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
            ui32 maxChunksToDefrag = DCtx->VCfg->MaxChunksToDefragInflight;

            if (!ChunksToDefrag) {
                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD07, DCtx->VCtx->VDiskLogPrefix << "going to find chunks to defrag",
                    (ActorId, SelfActorId));

                TDefragQuantumFindChunks findChunks(GetSnapshot(), DCtx->HugeBlobCtx);
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
                ChunksToDefrag.emplace(findChunks.GetChunksToDefrag(maxChunksToDefrag));
            }
            if (*ChunksToDefrag || ChunksToDefrag->IsShred) {
                const bool isShred = ChunksToDefrag->IsShred;

                TDefragChunks lockedChunks;

                if (!isShred) {
                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD09, DCtx->VCtx->VDiskLogPrefix
                        << "commencing defragmentation", (ActorId, SelfActorId), (ChunksToDefrag, *ChunksToDefrag));

                    stat.FoundChunksToDefrag = ChunksToDefrag->FoundChunksToDefrag;
                    stat.Eof = stat.FoundChunksToDefrag < maxChunksToDefrag;
                    stat.FreedChunks = ChunksToDefrag->Chunks;

                    lockedChunks = LockChunks(*ChunksToDefrag);

                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD11, DCtx->VCtx->VDiskLogPrefix << "locked chunks",
                        (ActorId, SelfActorId), (LockedChunks, lockedChunks));
                } else {
                    auto forbiddenChunks = GetForbiddenChunks();

                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD14, DCtx->VCtx->VDiskLogPrefix
                        << "commencing shredding", (ActorId, SelfActorId), (ChunksToShred, ChunksToDefrag->ChunksToShred),
                        (ForbiddenChunks, forbiddenChunks));

                    // filter chunks to shred via forbidden chunks
                    auto& chunksToShred = ChunksToDefrag->ChunksToShred;
                    for (const TChunkIdx chunkIdx : std::exchange(chunksToShred, {})) {
                        if (forbiddenChunks.contains(chunkIdx)) {
                            chunksToShred.insert(chunkIdx);
                        }
                    }

                    // check if we have something remaining to process
                    if (chunksToShred.empty()) {
                        STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD15, DCtx->VCtx->VDiskLogPrefix << "nothing to do",
                            (ActorId, SelfActorId), (Stat, stat));
                        Send(ParentActorId, new TEvDefragQuantumResult(std::move(stat)));
                        return;
                    }
                }

                TDefragQuantumFindRecords findRecords(DCtx->VCtx->VDiskLogPrefix,
                        std::move(*ChunksToDefrag), lockedChunks);
                while (findRecords.Scan(NDefrag::WorkQuantum, GetSnapshot())) {
                    Yield();
                }

                if (auto records = findRecords.GetRecordsToRewrite(); !records.empty()) {
                    THashMap<TChunkIdx, ui32> heatmap;
                    for (const auto& record : records) {
                        ++heatmap[record.OldDiskPart.ChunkIdx];
                    }

                    std::vector<std::tuple<TChunkIdx, ui32>> chunks(heatmap.begin(), heatmap.end());
                    std::ranges::sort(chunks, std::less<ui32>(), [](const auto& x) { return std::get<1>(x); });

                    const size_t numRecordsTotal = records.size();

                    if (isShred && chunks.size() > maxChunksToDefrag) {
                        chunks.resize(maxChunksToDefrag);
                        THashSet<TChunkIdx> set;
                        for (const auto& [chunkIdx, usage] : chunks) {
                            set.insert(chunkIdx);
                        }
                        auto pred = [&](const auto& record) { return !set.contains(record.OldDiskPart.ChunkIdx); };
                        auto range = std::ranges::remove_if(records, pred);
                        records.erase(range.begin(), range.end());
                        findRecords.SetLockedChunks(std::move(set));
                    }

                    auto getSortedChunks = [&] {
                        std::vector<TChunkIdx> temp;
                        temp.reserve(chunks.size());
                        for (const auto& [chunkIdx, usage] : chunks) {
                            temp.push_back(chunkIdx);
                        }
                        std::ranges::sort(temp);
                        return temp;
                    };

                    STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD12, DCtx->VCtx->VDiskLogPrefix << "rewriting records",
                        (ActorId, SelfActorId), (NumRecordsToRewrite, records.size()), (NumRecordsTotal, numRecordsTotal),
                        (Chunks, getSortedChunks()));

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
                    if (isShred) {
                        stat.Eof = false;
                    }
                }

                // scan index again to find tables we have to compact
                for (findRecords.StartFindingTablesToCompact(); findRecords.Scan(NDefrag::WorkQuantum, GetSnapshot()); Yield()) {}
                if (auto records = findRecords.GetRecordsToRewrite(); !records.empty()) {
                    for (const auto& item : records) {
                        STLOG(PRI_WARN, BS_VDISK_DEFRAG, BSVDD16, DCtx->VCtx->VDiskLogPrefix
                            << "blob found again after rewriting", (ActorId, SelfActorId), (Id, item.LogoBlobId),
                            (Location, item.OldDiskPart));
                    }
                }

                auto tablesToCompact = findRecords.GetTablesToCompact();
                const bool needsFreshCompaction = findRecords.GetNeedsFreshCompaction();
                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD13, DCtx->VCtx->VDiskLogPrefix << "compacting",
                    (ActorId, SelfActorId), (TablesToCompact, tablesToCompact),
                    (NeedsFreshCompaction, needsFreshCompaction));
                Compact(std::move(tablesToCompact), needsFreshCompaction);
            }

            STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD15, DCtx->VCtx->VDiskLogPrefix << "quantum finished",
                (ActorId, SelfActorId), (Stat, stat));

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

        THashSet<TChunkIdx> GetForbiddenChunks() {
            TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvHugeQueryForbiddenChunks, 0, DCtx->HugeKeeperId,
                SelfActorId, nullptr, 0));
            auto res = WaitForSpecificEvent<TEvHugeForbiddenChunks>(&TDefragQuantum::ProcessUnexpectedEvent);
            return res->Get()->ForbiddenChunks;
        }

        void Compact(THashSet<ui64> tablesToCompact, bool needsFreshCompaction) {
            if (tablesToCompact) {
                Send(DCtx->SkeletonId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, std::move(tablesToCompact)));
            } else if (needsFreshCompaction) {
                Send(DCtx->SkeletonId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, TEvCompactVDisk::EMode::FRESH_ONLY));
            } else {
                return; // nothing to do
            }
            WaitForSpecificEvent<TEvCompactVDiskResult>(&TDefragQuantum::ProcessUnexpectedEvent);
        }
    };

    IActor *CreateDefragQuantumActor(const std::shared_ptr<TDefragCtx>& dctx, const TVDiskID& selfVDiskId,
            std::optional<TChunksToDefrag> chunksToDefrag) {
        return new TActorCoro(MakeHolder<TDefragQuantum>(dctx, selfVDiskId, std::move(chunksToDefrag)),
            NKikimrServices::TActivity::BS_DEFRAG_QUANTUM);
    }

} // NKikimr

template<>
void Out<NKikimr::TEvDefragQuantumResult::TStat>(IOutputStream& s, const NKikimr::TEvDefragQuantumResult::TStat& x) {
    x.Output(s);
}
