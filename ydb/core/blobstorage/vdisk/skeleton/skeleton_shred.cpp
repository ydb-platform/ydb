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
        THashSet<ui32> ChunksShredded;
        THashSet<ui32> ChunksToShred;
        TPDiskCtxPtr PDiskCtx;
        const TActorId HugeKeeperId;
        const TActorId DefragId;
        TVDiskContextPtr VCtx;
        TActorId SkeletonId;
        NKikimrProto::EReplyStatus Status = NKikimrProto::EReplyStatus::ERROR;
        TString ErrorReason = "request aborted";

    public:
        TSkeletonShredActor(NPDisk::TEvShredVDisk::TPtr ev, TPDiskCtxPtr pdiskCtx, TActorId hugeKeeperId,
                TActorId defragId, TVDiskContextPtr vctx)
            : Sender(ev->Sender)
            , Cookie(ev->Cookie)
            , ShredGeneration(ev->Get()->ShredGeneration)
            , ChunksToShred(ev->Get()->ChunksToShred.begin(), ev->Get()->ChunksToShred.end())
            , PDiskCtx(std::move(pdiskCtx))
            , HugeKeeperId(hugeKeeperId)
            , DefragId(defragId)
            , VCtx(std::move(vctx))
        {}

        void Bootstrap(TActorId skeletonId) {
            SkeletonId = skeletonId;
            Become(&TThis::StateFunc);
            if (!ChunksToShred.empty()) {
                Send(HugeKeeperId, new TEvHugeShredNotify({ChunksToShred.begin(), ChunksToShred.end()}));
                Send(SkeletonId, new TEvTakeHullSnapshot(true)); // take index snapshot
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
            Send(DefragId, new TEvHullShredDefrag({ChunksToShred.begin(), ChunksToShred.end()}));
        }

        void HandleHullShredDefragResult() {
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr ev) {
            THullDsSnap& snap = ev->Get()->Snap;
            TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>::TForwardIterator iter(snap.HullCtx, &snap.LogoBlobsSnap);
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt(&iter);

            THashSet<TChunkIdx> chunksWithInlineBlobs;
            THashSet<TChunkIdx> chunksWithHugeBlobs;

            struct TMerger {
                TBlobStorageGroupType GType;
                const THashSet<TChunkIdx>& ChunksToShred;
                THashSet<TChunkIdx>& ChunksWithInlineBlobs;
                THashSet<TChunkIdx>& ChunksWithHugeBlobs;
                TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob> BaseMerger{GType};

                bool HaveToMergeData() const {
                    return BaseMerger.HaveToMergeData();
                }

                void Clear() {
                    BaseMerger.Clear();
                }

                void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key,
                        ui64 circaLsn, const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob> *sst) {
                    BaseMerger.AddFromSegment(memRec, outbound, key, circaLsn, sst);
                    ProcessData(memRec, outbound);
                }

                void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope *data, const TKeyLogoBlob& key, ui64 lsn) {
                    BaseMerger.AddFromFresh(memRec, data, key, lsn);
                    ProcessData(memRec, nullptr);
                }

                void ProcessData(TMemRecLogoBlob memRec, const TDiskPart *outbound) {
                    TDiskDataExtractor extr;
                    switch (const auto type = memRec.GetType()) {
                        case TBlobType::DiskBlob:
                        case TBlobType::HugeBlob:
                        case TBlobType::ManyHugeBlobs:
                            memRec.GetDiskData(&extr, outbound);
                            for (const TDiskPart *p = extr.Begin; p != extr.End; ++p) {
                                if (p->ChunkIdx && ChunksToShred.contains(p->ChunkIdx)) {
                                    auto *set = type == TBlobType::DiskBlob
                                        ? &ChunksWithInlineBlobs
                                        : &ChunksWithHugeBlobs;
                                    set->insert(p->ChunkIdx);
                                }
                            }
                            break;

                        case TBlobType::MemBlob:
                            break;
                    }
                }

                void Finish() {
                    BaseMerger.Finish();
                }
            } merger{
                .GType = VCtx->Top->GType,
                .ChunksToShred = ChunksToShred,
                .ChunksWithInlineBlobs = chunksWithInlineBlobs,
                .ChunksWithHugeBlobs = chunksWithHugeBlobs,
            };

            heapIt.Walk(std::nullopt, &merger, [&](TKeyLogoBlob /*key*/, auto* /*merger*/) { return true; });
        }

        void Handle(TEvNotifyChunksDeleted::TPtr ev) {
            for (ui32 chunkId : ev->Get()->Chunks) {
                if (ChunksToShred.erase(chunkId)) {
                    ChunksShredded.insert(chunkId);
                }
            }
            CheckIfDone();
        }

        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            std::vector<TChunkIdx> chunksToShred(ChunksToShred.begin(), ChunksToShred.end());
            std::ranges::sort(chunksToShred);

            std::vector<TChunkIdx> chunksShredded(ChunksShredded.begin(), ChunksShredded.end());
            std::ranges::sort(chunksShredded);

            TStringStream s;
            s << "ShredGeneration# " << ShredGeneration
                << " ChunksToShred# " << FormatList(chunksToShred)
                << " ChunksShredded# " << FormatList(chunksShredded)
                << "<br/>";
            Send(ev->Sender, new NMon::TEvHttpInfoRes(s.Str(), ev->Get()->SubRequestId));
        }

        void PassAway() override {
            Send(Sender, new NPDisk::TEvShredVDiskResult(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound,
                ShredGeneration, Status, std::move(ErrorReason)), 0, Cookie);
            Send(SkeletonId, new TEvents::TEvGone);
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvBlobStorage::EvHugeShredNotifyResult, HandleHugeShredNotifyResult)
            cFunc(TEvBlobStorage::EvHullShredDefragResult, HandleHullShredDefragResult)
            hFunc(TEvTakeHullSnapshotResult, Handle)
            hFunc(TEvNotifyChunksDeleted, Handle)
            hFunc(NMon::TEvHttpInfo, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    IActor *CreateSkeletonShredActor(NPDisk::TEvShredVDisk::TPtr ev, TPDiskCtxPtr pdiskCtx, TActorId hugeKeeperId,
            TActorId defragId, TVDiskContextPtr vctx) {
        return new TSkeletonShredActor(ev, std::move(pdiskCtx), hugeKeeperId, defragId, std::move(vctx));
    }

} // NKikimr
