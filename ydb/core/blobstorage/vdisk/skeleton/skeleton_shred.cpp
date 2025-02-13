#include "skeleton_shred.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>

namespace NKikimr {

    class TSkeletonShredActor : public TActorBootstrapped<TSkeletonShredActor> {
        const TActorId Sender;
        const ui64 Cookie;
        const ui64 ShredGeneration;
        THashSet<TChunkIdx> ChunksToShred;
        TShredCtxPtr ShredCtx;
        NKikimrProto::EReplyStatus Status = NKikimrProto::EReplyStatus::ERROR;
        TString ErrorReason = "request aborted";

        enum class EChunkType {
            UNKNOWN,
            HUGE_CHUNK,
            SYNCLOG,
            INDEX,
        };

        THashMap<TChunkIdx, EChunkType> ChunkTypes;
        THashSet<TChunkIdx> ChunksShredded;
        THashSet<ui64> TablesToCompact;
        ui32 RepliesPending = 0;
        bool SnapshotProcessed = false;
        bool DefragCompleted = false;
        bool FoundAnyChunks = false;

    public:
        TSkeletonShredActor(NPDisk::TEvShredVDisk::TPtr ev, TShredCtxPtr shredCtx)
            : Sender(ev->Sender)
            , Cookie(ev->Cookie)
            , ShredGeneration(ev->Get()->ShredGeneration)
            , ChunksToShred(ev->Get()->ChunksToShred.begin(), ev->Get()->ChunksToShred.end())
            , ShredCtx(std::move(shredCtx))
        {
            for (const TChunkIdx chunkId : ChunksToShred) {
                ChunkTypes.emplace(chunkId, EChunkType::UNKNOWN);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
            STLOG(PRI_DEBUG, BS_SHRED, BSSV05, ShredCtx->VCtx->VDiskLogPrefix << "TSkeletonShredActor bootstrap",
                (ActorId, SelfId()), (ChunksToShred, ChunksToShred), (ShredGeneration, ShredGeneration),
                (Lsn, ShredCtx->Lsn));
            if (!ChunksToShred.empty()) {
                Send(ShredCtx->HugeKeeperId, new TEvHugeShredNotify({ChunksToShred.begin(), ChunksToShred.end()}));
            }
            CheckIfDone();
        }

        void CheckIfDone() {
            if (ChunksToShred.empty()) {
                Status = NKikimrProto::OK;
                ErrorReason = {};
                PassAway();
            }
        }

        void HandleHugeShredNotifyResult() {
            STLOG(PRI_DEBUG, BS_SHRED, BSSV06, ShredCtx->VCtx->VDiskLogPrefix << "EvHugeShredNotifyResult received or"
                " timer hit", (ActorId, SelfId()));
            Send(ShredCtx->DefragId, new TEvHullShredDefrag(ChunksToShred));
            Send(ShredCtx->HugeKeeperId, new TEvListChunks(ChunksToShred));
            Send(ShredCtx->SyncLogId, new TEvListChunks(ChunksToShred));
            RepliesPending = 2;
            SnapshotProcessed = false;
            DefragCompleted = false;
            FoundAnyChunks = false;
        }

        void Handle(TEvListChunksResult::TPtr ev) {
            auto *msg = ev->Get();

            STLOG(PRI_DEBUG, BS_SHRED, BSSV07, ShredCtx->VCtx->VDiskLogPrefix << "TEvListChunksResult received",
                (ActorId, SelfId()), (ChunksHuge, msg->ChunksHuge), (ChunksSyncLog, msg->ChunksSyncLog));

            auto update = [&](const auto& set, auto type) {
                for (const TChunkIdx chunkId : set) {
                    if (const auto it = ChunkTypes.find(chunkId); it != ChunkTypes.end()) {
                        it->second = type;
                        FoundAnyChunks = true;
                        Y_DEBUG_ABORT_UNLESS(ChunksToShred.contains(chunkId));
                    } else {
                        Y_DEBUG_ABORT_UNLESS(!ChunksToShred.contains(chunkId));
                    }
                }
            };
            update(msg->ChunksHuge, EChunkType::HUGE_CHUNK);
            update(msg->ChunksSyncLog, EChunkType::SYNCLOG);

            if (!--RepliesPending) {
                Send(ShredCtx->SkeletonId, new TEvTakeHullSnapshot(true));
            }
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr ev) {
            STLOG(PRI_DEBUG, BS_SHRED, BSSV08, ShredCtx->VCtx->VDiskLogPrefix << "TEvTakeHullSnapshotResult received",
                (ActorId, SelfId()));

            auto& snap = ev->Get()->Snap;
            TablesToCompact.clear();
            Scan<true>(snap.HullCtx, snap.LogoBlobsSnap, TablesToCompact);
            Scan<false>(snap.HullCtx, snap.BlocksSnap, TablesToCompact);
            Scan<false>(snap.HullCtx, snap.BarriersSnap, TablesToCompact);
            SnapshotProcessed = true;
            CheckDefragStage();

            STLOG(PRI_DEBUG, BS_SHRED, BSSV09, ShredCtx->VCtx->VDiskLogPrefix << "TEvTakeHullSnapshotResult processed",
                (ActorId, SelfId()), (TablesToCompact, TablesToCompact));
        }

        template<bool Blobs, typename TKey, typename TMemRec>
        void Scan(const TIntrusivePtr<THullCtx>& hullCtx, TLevelIndexSnapshot<TKey, TMemRec>& snap,
                THashSet<ui64>& tablesToCompact) {
            auto scanHuge = [&](const TMemRec& memRec, const TDiskPart *outbound) {
                if (memRec.GetType() == TBlobType::HugeBlob || memRec.GetType() == TBlobType::ManyHugeBlobs) {
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, outbound);
                    for (const TDiskPart *p = extr.Begin; p != extr.End; ++p) {
                        if (p->Empty()) {
                            continue;
                        }
                        if (const auto it = ChunkTypes.find(p->ChunkIdx); it != ChunkTypes.end()) {
                            it->second = EChunkType::HUGE_CHUNK;
                            FoundAnyChunks = true;
                        }
                    }
                }
            };

            auto scanFresh = [&](const auto& seg) {
                typename std::decay_t<decltype(seg)>::TIteratorWOMerge it(hullCtx, &seg);
                for (it.SeekToFirst(); it.Valid(); it.Next()) {
                    scanHuge(it.GetUnmergedMemRec(), nullptr);
                }
            };
            scanFresh(snap.FreshSnap.Cur);
            scanFresh(snap.FreshSnap.Dreg);
            scanFresh(snap.FreshSnap.Old);

            typename TLevelSliceSnapshot<TKey, TMemRec>::TSstIterator sstIt(&snap.SliceSnap);
            for (sstIt.SeekToFirst(); sstIt.Valid(); sstIt.Next()) {
                const auto& p = sstIt.Get();
                const auto& seg = *p.SstPtr;

                for (const TChunkIdx chunkId : seg.AllChunks) {
                    if (const auto it = ChunkTypes.find(chunkId); it != ChunkTypes.end()) {
                        it->second = EChunkType::INDEX;
                        tablesToCompact.insert(seg.AssignedSstId);
                        STLOG(PRI_DEBUG, BS_SHRED, BSSV13, ShredCtx->VCtx->VDiskLogPrefix << "going to compact SST",
                            (SstId, seg.AssignedSstId), (AllChunks, seg.AllChunks));
                        FoundAnyChunks = true;
                        Y_DEBUG_ABORT_UNLESS(ChunksToShred.contains(chunkId));
                    } else {
                        Y_DEBUG_ABORT_UNLESS(!ChunksToShred.contains(chunkId));
                    }
                }

                if constexpr (Blobs) {
                    const TDiskPart *outbound = seg.GetOutbound();
                    typename TLevelSegment<TKey, TMemRec>::TMemIterator memIt(&seg);
                    for (memIt.SeekToFirst(); memIt.Valid(); memIt.Next()) {
                        scanHuge(memIt->MemRec, outbound);
                    }
                }
            }
        }

        void HandleHullShredDefragResult() {
            STLOG(PRI_DEBUG, BS_SHRED, BSSV14, ShredCtx->VCtx->VDiskLogPrefix << "EvHullShredDefragResult received",
                (ActorId, SelfId()));
            DefragCompleted = true;
            CheckDefragStage();
        }

        void CheckDefragStage() {
            if (!SnapshotProcessed || !DefragCompleted) {
                return;
            }

            if (!TablesToCompact.empty()) {
                Send(ShredCtx->SkeletonId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, std::move(TablesToCompact)));
            } else {
                TActivationContext::Schedule(TDuration::Minutes(1), new IEventHandle(TEvents::TSystem::Wakeup, 0,
                    SelfId(), TActorId(), nullptr, 0));
            }
        }

        void Handle(TEvCompactVDiskResult::TPtr /*ev*/) {
            STLOG(PRI_DEBUG, BS_SHRED, BSSV11, ShredCtx->VCtx->VDiskLogPrefix << "TEvCompactVDiskResult received",
                (ActorId, SelfId()));
            TActivationContext::Schedule(TDuration::Minutes(1), new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(),
                TActorId(), nullptr, 0));
        }

        void Handle(TEvNotifyChunksDeleted::TPtr ev) {
            STLOG(PRI_DEBUG, BS_SHRED, BSSV10, ShredCtx->VCtx->VDiskLogPrefix << "TEvNotifyChunksDeleted received",
                (ActorId, SelfId()), (Lsn, ev->Get()->Lsn), (Chunks, ev->Get()->Chunks));

            if (ShredCtx->Lsn < ev->Get()->Lsn) { // don't accept stale queries
                for (ui32 chunkId : ev->Get()->Chunks) {
                    if (ChunksToShred.erase(chunkId)) {
                        ChunksShredded.insert(chunkId);
                        ChunkTypes.erase(chunkId);
                    }
                }
                TActivationContext::Send(IEventHandle::Forward(ev, ShredCtx->DefragId));
                CheckIfDone();
            }
        }

        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            std::vector<TChunkIdx> chunksToShred(ChunksToShred.begin(), ChunksToShred.end());
            std::ranges::sort(chunksToShred);

            std::vector<TChunkIdx> chunksShredded(ChunksShredded.begin(), ChunksShredded.end());
            std::ranges::sort(chunksShredded);

            TStringStream s;
            HTML(s) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        s << "Shred State";
                    }
                    DIV_CLASS("panel-body") {
                        DIV() {
                            s << "ShredGeneration# " << ShredGeneration << "<br/>";
                        }
                        DIV() {
                            s << "ChunksToShred# [";
                            for (const char *sp = ""; TChunkIdx chunkId : ChunksToShred) {
                                const auto it = ChunkTypes.find(chunkId);
                                Y_ABORT_UNLESS(it != ChunkTypes.end());
                                const char *color = "gray";
                                switch (it->second) {
                                    case EChunkType::UNKNOWN:
                                        break;

                                    case EChunkType::HUGE_CHUNK:
                                        color = "red";
                                        break;

                                    case EChunkType::INDEX:
                                        color = "gold";
                                        break;

                                    case EChunkType::SYNCLOG:
                                        color = "blue";
                                        break;
                                }
                                s << std::exchange(sp, " ") << "<font color=" << color << ">" << chunkId << "</font>";
                            }
                            s << "]<br/>";
                        }
                        DIV() {
                            s << "ChunksShredded# " << FormatList(chunksShredded) << "<br/>";
                        }
                    }
                }
            }
            Send(ev->Sender, new NMon::TEvHttpInfoRes(s.Str(), ev->Get()->SubRequestId));
        }

        void PassAway() override {
            STLOG(PRI_INFO, BS_SHRED, BSSV12, ShredCtx->VCtx->VDiskLogPrefix << "shredding finished",
                (ActorId, SelfId()), (Status, Status), (ErrorReason, ErrorReason), (ChunksShredded, ChunksShredded));
            Send(Sender, new NPDisk::TEvShredVDiskResult(ShredCtx->PDiskCtx->Dsk->Owner,
                ShredCtx->PDiskCtx->Dsk->OwnerRound, ShredGeneration, Status, std::move(ErrorReason)), 0, Cookie);
            Send(ShredCtx->SkeletonId, new TEvents::TEvGone);
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvBlobStorage::EvHugeShredNotifyResult, HandleHugeShredNotifyResult)
            hFunc(TEvListChunksResult, Handle)
            hFunc(TEvTakeHullSnapshotResult, Handle)
            cFunc(TEvBlobStorage::EvHullShredDefragResult, HandleHullShredDefragResult)
            hFunc(TEvCompactVDiskResult, Handle)
            hFunc(TEvNotifyChunksDeleted, Handle)
            cFunc(TEvents::TSystem::Wakeup, HandleHugeShredNotifyResult)
            hFunc(NMon::TEvHttpInfo, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    IActor *CreateSkeletonShredActor(NPDisk::TEvShredVDisk::TPtr ev, TShredCtxPtr shredCtx) {
        return new TSkeletonShredActor(ev, std::move(shredCtx));
    }

} // NKikimr
