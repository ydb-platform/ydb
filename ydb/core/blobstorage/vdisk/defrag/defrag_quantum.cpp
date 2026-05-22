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
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_DEFRAG

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
            YDB_LOG_DEBUG("defrag quantum start",
                {"Marker", "BSVDD00"},
                {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                {"ActorId", SelfActorId});

            try {
                RunImpl();
            } catch (const TExPoison&) {
            }

            YDB_LOG_DEBUG("defrag quantum end",
                {"Marker", "BSVDD06"},
                {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                {"ActorId", SelfActorId});
        }

        void RunImpl() {
            TEvDefragQuantumResult::TStat stat{.Eof = true};
            ui32 maxChunksToDefrag = DCtx->VCfg->MaxChunksToDefragInflight;

            if (!ChunksToDefrag) {
                YDB_LOG_DEBUG("going to find chunks to defrag",
                    {"Marker", "BSVDD07"},
                    {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                    {"ActorId", SelfActorId});

                TDefragQuantumFindChunks findChunks(GetSnapshot(), DCtx->HugeBlobCtx);
                const ui64 endTime = GetCycleCountFast() + DurationToCycles(NDefrag::MaxSnapshotHoldDuration);
                while (findChunks.Scan(NDefrag::WorkQuantum)) {
                    if (GetCycleCountFast() >= endTime) {
                        YDB_LOG_DEBUG("timed out while finding chunks to defrag",
                            {"Marker", "BSVDD08"},
                            {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                            {"ActorId", SelfActorId},
                            {"Stat", stat});
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
                    YDB_LOG_DEBUG("commencing defragmentation",
                        {"Marker", "BSVDD09"},
                        {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                        {"ActorId", SelfActorId},
                        {"ChunksToDefrag", *ChunksToDefrag});

                    stat.FoundChunksToDefrag = ChunksToDefrag->FoundChunksToDefrag;
                    stat.Eof = stat.FoundChunksToDefrag < maxChunksToDefrag;
                    stat.FreedChunks = ChunksToDefrag->Chunks;

                    lockedChunks = LockChunks(*ChunksToDefrag);

                    YDB_LOG_DEBUG("locked chunks",
                        {"Marker", "BSVDD11"},
                        {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                        {"ActorId", SelfActorId},
                        {"LockedChunks", lockedChunks});

                    if (lockedChunks.empty()) {
                        STLOG(PRI_NOTICE, BS_VDISK_DEFRAG, BSVDD17, DCtx->VCtx->VDiskLogPrefix << "could not lock chunks, going to run full compaction instead", (ChunksToDefrag, *ChunksToDefrag));
                    }
                } else {
                    auto forbiddenChunks = GetForbiddenChunks();

                    YDB_LOG_DEBUG("commencing shredding",
                        {"Marker", "BSVDD14"},
                        {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                        {"ActorId", SelfActorId},
                        {"ChunksToShred", ChunksToDefrag->ChunksToShred},
                        {"ForbiddenChunks", forbiddenChunks});

                    // filter chunks to shred via forbidden chunks
                    auto& chunksToShred = ChunksToDefrag->ChunksToShred;
                    for (const TChunkIdx chunkIdx : std::exchange(chunksToShred, {})) {
                        if (forbiddenChunks.contains(chunkIdx)) {
                            chunksToShred.insert(chunkIdx);
                        }
                    }

                    // check if we have something remaining to process
                    if (chunksToShred.empty()) {
                        YDB_LOG_DEBUG("nothing to do",
                            {"Marker", "BSVDD15"},
                            {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                            {"ActorId", SelfActorId},
                            {"Stat", stat});
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

                    YDB_LOG_DEBUG("rewriting records",
                        {"Marker", "BSVDD12"},
                        {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                        {"ActorId", SelfActorId},
                        {"NumRecordsToRewrite", records.size()},
                        {"NumRecordsTotal", numRecordsTotal},
                        {"Chunks", getSortedChunks()});

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

                if (DCtx->VCfg->GarbageThresholdToRunFullCompactionPerMille == 0 || (!isShred && lockedChunks.empty())) {
                    // scan index again to find tables we have to compact
                    for (findRecords.StartFindingTablesToCompact(); findRecords.Scan(NDefrag::WorkQuantum, GetSnapshot()); Yield()) {}
                    if (auto records = findRecords.GetRecordsToRewrite(); !records.empty()) {
                        for (const auto& item : records) {
                            YDB_LOG_WARN("blob found again after rewriting",
                                {"Marker", "BSVDD16"},
                                {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                                {"ActorId", SelfActorId},
                                {"Id", item.LogoBlobId},
                                {"Location", item.OldDiskPart});
                        }
                    }

                    auto tablesToCompact = findRecords.GetTablesToCompact();
                    const bool needsFreshCompaction = findRecords.GetNeedsFreshCompaction();
                    YDB_LOG_DEBUG("compacting",
                        {"Marker", "BSVDD13"},
                        {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                        {"ActorId", SelfActorId},
                        {"TablesToCompact", tablesToCompact},
                        {"NeedsFreshCompaction", needsFreshCompaction});
                    Compact(std::move(tablesToCompact), needsFreshCompaction);
                }
            }

            YDB_LOG_DEBUG("quantum finished",
                {"Marker", "BSVDD15"},
                {"VDiskLogPrefix", DCtx->VCtx->VDiskLogPrefix},
                {"ActorId", SelfActorId},
                {"Stat", stat});

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
                Send(DCtx->SkeletonId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, std::move(tablesToCompact)), false);
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
