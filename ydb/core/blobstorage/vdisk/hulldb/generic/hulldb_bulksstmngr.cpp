#include "hulldb_bulksstmngr.h"
#include "hulldb_bulksstloaded.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullload.h>

namespace NKikimr {

    namespace NLoaderActor {
        using TLevelSegment = NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
        using THullSegLoaded = NKikimr::THullSegLoaded<TLevelSegment>;

        class TLoaderActor : public TActorBootstrapped<TLoaderActor> {
            struct TBulkSegmentLoadQueueItem {
                TLevelSegmentPtr Segment;
                ui64             FirstLsn;
                ui64             LastLsn;

                TBulkSegmentLoadQueueItem(TLevelSegmentPtr segment, ui64 firstLsn, ui64 lastLsn)
                    : FirstLsn(firstLsn)
                    , LastLsn(lastLsn)
                {
                    Segment.Swap(segment);
                }
            };

            TVDiskContextPtr VCtx;
            TPDiskCtxPtr PDiskCtx;
            const TActorId LocalRecoveryActorId;
            TQueue<TBulkSegmentLoadQueueItem> BulkSegmentLoadQueue;
            THashMap<TActorId, TBulkSegmentLoadQueueItem> BulkSegmentLoadInFlight;
            TVector<TLevelSegmentPtr> Segments;
            TActiveActors ActiveActors;

        public:
            static constexpr auto ActorActivityType() {
                return NKikimrServices::TActivity::BS_BULK_SST_LOADER;
            }

            TLoaderActor(TVDiskContextPtr &&vctx, TPDiskCtxPtr &&pdiskCtx, const TActorId& localRecoveryActorId)
                : VCtx(std::move(vctx))
                , PDiskCtx(std::move(pdiskCtx))
                , LocalRecoveryActorId(localRecoveryActorId)
            {}

            void AddQueueItem(const TBulkFormedSstInfo& seg) {
                TLevelSegmentPtr segment = new TLevelSegment(VCtx, seg.EntryPoint);
                BulkSegmentLoadQueue.emplace(segment, seg.FirstBlobLsn, seg.LastBlobLsn);
            }

        private:
            friend class TActorBootstrapped<TLoaderActor>;

            void Bootstrap(const TActorContext& ctx) {
                IssueLoadRequestsOrFinish(ctx);
                Become(&TLoaderActor::StateFunc);
            }

            void IssueLoadRequestsOrFinish(const TActorContext& ctx) {
                while (!BulkSegmentLoadQueue.empty() && BulkSegmentLoadInFlight.size() < 3) {
                    TBulkSegmentLoadQueueItem& item = BulkSegmentLoadQueue.front();
                    auto loader = std::make_unique<TLevelSegmentLoader<TKeyLogoBlob, TMemRecLogoBlob>>(VCtx,
                            PDiskCtx, item.Segment.Get(), ctx.SelfID, "BulkSegsLoader");
                    TActorId loaderId = ctx.ExecutorThread.RegisterActor(loader.release());
                    ActiveActors.Insert(loaderId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                    BulkSegmentLoadInFlight.emplace(loaderId, std::move(item));
                    BulkSegmentLoadQueue.pop();
                }
                if (BulkSegmentLoadQueue.empty() && BulkSegmentLoadInFlight.empty()) {
                    Finish(ctx);
                }
            }

            void Handle(THullSegLoaded::TPtr& ev, const TActorContext& ctx) {
                ActiveActors.Erase(ev->Sender);
                auto i = BulkSegmentLoadInFlight.find(ev->Sender);
                Y_ABORT_UNLESS(i != BulkSegmentLoadInFlight.end());
                TBulkSegmentLoadQueueItem& item = i->second;

                // check that we have loaded correct segment :)
                Y_ABORT_UNLESS(ev->Get()->LevelSegment == item.Segment.Get());

                // fill in loaded segment's LSN range
                TLevelSegment& seg = *item.Segment;
                seg.Info.FirstLsn = item.FirstLsn;
                seg.Info.LastLsn = item.LastLsn;

                // push segment into results
                Segments.push_back(std::move(item.Segment));

                // remove item from in flight set
                BulkSegmentLoadInFlight.erase(i);

                // send more requests
                IssueLoadRequestsOrFinish(ctx);
            }

            void Finish(const TActorContext& ctx) {
                ctx.Send(LocalRecoveryActorId, new TEvBulkSstsLoaded{std::move(Segments)});
                Die(ctx);
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                ActiveActors.KillAndClear(ctx);
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(THullSegLoaded, Handle)
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
            )
        };
    } // NLoaderActor

    TBulkFormedSstInfoSet::TBulkFormedSstInfoSet(const NKikimrVDiskData::TBulkFormedSstInfoSet& pb) {
        const auto& segments = pb.GetSegments();
        BulkFormedSsts = {segments.begin(), segments.end()};
        std::sort(BulkFormedSsts.begin(), BulkFormedSsts.end());
    }

    void TBulkFormedSstInfoSet::RemoveSstFromIndex(const TDiskPart& entryPoint) {
        TBulkFormedSstInfo& bulk = FindIntactBulkFormedSst(entryPoint);
        Y_ABORT_UNLESS(!bulk.RemovedFromIndex && bulk.ChunkIds.empty());
        bulk.RemovedFromIndex = true;
    }

    void TBulkFormedSstInfoSet::ApplyCompactionResult(TBulkFormedSstInfoSet& output, TVector<TChunkIdx>& deleteChunks) {
        Y_ABORT_UNLESS(output.BulkFormedSsts.empty());
        for (TBulkFormedSstInfo& seg : std::exchange(BulkFormedSsts, {})) {
            if (!seg.RemovedFromIndex) {
                output.BulkFormedSsts.push_back(std::move(seg));
            } else if (seg.ChunkIds) {
                AppendToVector(deleteChunks, std::exchange(seg.ChunkIds, {}));
            }
        }
    }

    void TBulkFormedSstInfoSet::AddBulkFormedSst(ui64 firstLsn, ui64 lastLsn, const TDiskPart& entryPoint) {
        Y_ABORT_UNLESS(firstLsn && lastLsn && !entryPoint.Empty(),
            "firstLsn# %" PRIu64 " lastLsn# %" PRIu64 " entryPoint# %s",
            firstLsn, lastLsn, entryPoint.ToString().data());
        // keep sorted order while inserting new item
        const auto it = std::lower_bound(BulkFormedSsts.begin(), BulkFormedSsts.end(), entryPoint);
        BulkFormedSsts.emplace(it, firstLsn, lastLsn, entryPoint);
    }

    IActor *TBulkFormedSstInfoSet::CreateLoaderActor(TVDiskContextPtr vctx,
            TPDiskCtxPtr pdiskCtx,
            ui64 syncLogMaxLsnStored,
            const TActorId& localRecoveryActorId) {
        auto loader = std::make_unique<NLoaderActor::TLoaderActor>(std::move(vctx), std::move(pdiskCtx),
                localRecoveryActorId);

        for (const TBulkFormedSstInfo& seg : BulkFormedSsts) {
            // ignore segments which are not needed to recover SyncLog -- their LSNs are obsolete
            if (seg.LastBlobLsn <= syncLogMaxLsnStored) {
                continue;
            }
            // ignore segments that are not yet removed from index -- they were already loaded with index and
            // added to BulkFormedBlobSorter a few lines above
            if (!seg.RemovedFromIndex) {
                continue;
            }
            loader->AddQueueItem(seg);
        }

        return loader.release();
    }

    void TBulkFormedSstInfoSet::SerializeToProto(NKikimrVDiskData::TBulkFormedSstInfoSet &pb) const {
        for (const auto &x : BulkFormedSsts) {
            auto p = pb.AddSegments();
            x.SerializeToProto(*p);
        }
    }

    bool TBulkFormedSstInfoSet::ConvertToProto(NKikimrVDiskData::TBulkFormedSstInfoSet &pb,
            const char *begin, const char *end) {
        return pb.ParseFromArray(begin, end - begin);
    }

    const TBulkFormedSstInfo& TBulkFormedSstInfoSet::FindIntactBulkFormedSst(const TDiskPart& entryPoint) const {
        return FindIntactBulkFormedSstPrivate(entryPoint);
    }

    TBulkFormedSstInfo& TBulkFormedSstInfoSet::FindIntactBulkFormedSst(const TDiskPart& entryPoint) {
        return const_cast<TBulkFormedSstInfo&>(
            static_cast<const TBulkFormedSstInfoSet&>(*this).FindIntactBulkFormedSstPrivate(entryPoint));
    }

    const TBulkFormedSstInfo& TBulkFormedSstInfoSet::FindIntactBulkFormedSstPrivate(const TDiskPart& entryPoint) const {
        for (auto it = std::lower_bound(BulkFormedSsts.begin(), BulkFormedSsts.end(), entryPoint);
                it != BulkFormedSsts.end() && it->EntryPoint == entryPoint; ++it) {
            if (!it->RemovedFromIndex) {
                return *it;
            }
        }
        Y_ABORT("bulk-formed SSTable not found");
    }

    void TBulkFormedSstInfoSet::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
        for (const TBulkFormedSstInfo& seg : BulkFormedSsts) {
            seg.GetOwnedChunks(chunks);
        }
    }

} // NKikimr
