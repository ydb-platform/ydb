#include "barriers_public.h"
#include "barriers_tree.h"
#include "barriers_essence.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>

namespace NKikimr {
    namespace NBarriers {

        /////////////////////////////////////////////////////////////////////////////////////////////
        // TBarriersDsSnapshot
        /////////////////////////////////////////////////////////////////////////////////////////////
        TIntrusivePtr<TBarriersDsSnapshot::TBarriersEssence> TBarriersDsSnapshot::CreateEssence(
                const THullCtxPtr &hullCtx) const {
            return TBarriersDsSnapshot::TBarriersEssence::Create(hullCtx, *this);
        }

        TIntrusivePtr<TBarriersDsSnapshot::TBarriersEssence> TBarriersDsSnapshot::CreateEssence(
                const THullCtxPtr &hullCtx,
                ui64 firstTabletId,
                ui64 lastTabletId,
                int debugLevel) const {
            return TBarriersDsSnapshot::TBarriersEssence::Create(hullCtx, *this,
                firstTabletId, lastTabletId, debugLevel);
        }

        /////////////////////////////////////////////////////////////////////////////////////////////
        // TBarriersDs
        /////////////////////////////////////////////////////////////////////////////////////////////
        TBarriersDs::TBarriersDs(const TLevelIndexSettings &settings, std::shared_ptr<TRopeArena> arena)
            : TBase(settings, std::move(arena))
            , VDiskLogPrefix(settings.HullCtx->VCtx->VDiskLogPrefix)
            , MemView(std::make_unique<TMemView>(
                TIngressCache::Create(settings.HullCtx->VCtx->Top, settings.HullCtx->VCtx->ShortSelfVDisk),
                settings.HullCtx->VCtx->VDiskLogPrefix,
                settings.HullCtx->GCOnlySynced))
        {}

        TBarriersDs::TBarriersDs(
                const TLevelIndexSettings &settings,
                const NKikimrVDiskData::TLevelIndex &pb,
                ui64 entryPointLsn,
                std::shared_ptr<TRopeArena> arena)
            : TBase(settings, pb, entryPointLsn, std::move(arena))
            , VDiskLogPrefix(settings.HullCtx->VCtx->VDiskLogPrefix)
            , MemView(std::make_unique<TMemView>(
                TIngressCache::Create(settings.HullCtx->VCtx->Top, settings.HullCtx->VCtx->ShortSelfVDisk),
                settings.HullCtx->VCtx->VDiskLogPrefix,
                settings.HullCtx->GCOnlySynced))
        {}


        void TBarriersDs::PutToFresh(ui64 lsn, const TKeyBarrier &key, const TMemRecBarrier &memRec) {
            MemView->Update(key, memRec);
            TBase::PutToFresh(lsn, key, memRec);
        }

        void TBarriersDs::PutToFresh(std::shared_ptr<TBase::TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn) {
             Y_DEBUG_ABORT_UNLESS(a);

             // update barriers cache with newly inserted elements
             TFreshAppendix::TIterator it(Settings.HullCtx, a.get());
             it.SeekToFirst();
             while (it.Valid()) {
                 MemView->Update(it.GetCurKey(), it.GetMemRec());
                 it.Next();
             }

            TBase::PutToFresh(std::move(a), firstLsn, lastLsn);
        }

        void TBarriersDs::LoadCompleted() {
            TBase::LoadCompleted();
            BuildMemView();
        }

        TBarriersDsSnapshot TBarriersDs::GetSnapshot(TActorSystem *as) {
            return TBarriersDsSnapshot(TBase::GetSnapshot(as), MemView->GetSnapshot());
        }

        TBarriersDsSnapshot TBarriersDs::GetIndexSnapshot() {
            return TBarriersDsSnapshot(TBase::GetIndexSnapshot(), MemView->GetSnapshot());
        }

        void TBarriersDs::BuildMemView() {
            TBase::TLevelIndexSnapshot snap = TBase::GetIndexSnapshot();
            TBase::TLevelIndexSnapshot::TForwardIterator it(Settings.HullCtx, &snap);
            THeapIterator<TKeyBarrier, TMemRecBarrier, true> heapIt(&it);
            TIndexRecordMerger<TKeyBarrier, TMemRecBarrier> merger(Settings.HullCtx->VCtx->Top->GType);
            auto callback = [&] (TKeyBarrier key, auto* merger) -> bool {
                const TMemRecBarrier& memRec = merger->GetMemRec();
                MemView->Update(key, memRec);
                return true;
            };
            heapIt.Walk(TKeyBarrier::First(), &merger, callback);
        }

    } // NBarriers
} // NKikimr
