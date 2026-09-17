#pragma once
#include "defs.h"

#include "barriers_tree.h"
#include "barriers_essence.h"
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_appendix.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h>

namespace NKikimr {

    namespace NGcOpt {
        class TBarriersEssence;
    } // NGcOpt

    namespace NBarriers {

        using TBarriersSst = TLevelSegment<TKeyBarrier, TMemRecBarrier>;

        /////////////////////////////////////////////////////////////////////////////////////////////
        // TBarriersDsSnapshot
        // Barriers Datastructure Snapshot
        /////////////////////////////////////////////////////////////////////////////////////////////
        class TBarriersDsSnapshot : public TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier> {
        public:
            using TBarriersEssence = ::NKikimr::NGcOpt::TBarriersEssence;
        protected:
            friend class TBarriersDs;
            using TBase = TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier>;

            TBarriersDsSnapshot(TBase &&base, TMemViewSnap &&memViewSnap)
                : TBase(std::move(base))
                , MemViewSnap(std::move(memViewSnap))
            {}

        public:
            TIntrusivePtr<TBarriersEssence> CreateEssence(const THullCtxPtr &hullCtx) const;
            TIntrusivePtr<TBarriersEssence> CreateEssence(
                    const THullCtxPtr &hullCtx,
                    ui64 firstTabletId,         // 0 -- first tabletId
                    ui64 lastTabletId,          // Max<ui64>() -- last tabletId
                    int debugLevel) const;      // 0 -- by default
             TMemViewSnap GetMemViewSnap() const { return MemViewSnap; }

        private:
            TMemViewSnap MemViewSnap;
        };

        /////////////////////////////////////////////////////////////////////////////////////////////
        // TBarriersDs
        // Barriers Datastructure
        /////////////////////////////////////////////////////////////////////////////////////////////
        class TBarriersDs : public TLevelIndex<TKeyBarrier, TMemRecBarrier> {
        public:
            using TBase = TLevelIndex<TKeyBarrier, TMemRecBarrier>;

            TBarriersDs(const TLevelIndexSettings &settings, std::shared_ptr<TRopeArena> arena);
            TBarriersDs(
                    const TLevelIndexSettings &settings,
                    const NKikimrVDiskData::TLevelIndex &pb,
                    ui64 entryPointLsn,
                    std::shared_ptr<TRopeArena> arena);


            void PutToFresh(ui64 lsn, const TKeyBarrier &key, const TMemRecBarrier &memRec);
            void PutToFresh(std::shared_ptr<TBase::TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn);
            void LoadCompleted() override;
            void MarkTabletDeleted(ui64 tabletId);
            void MarkTabletsDeleted(const THashSet<ui64> &tabletIds);
            // Apply the records of an SST that was inserted into the level index directly (bulk/full
            // sync) -- those do not go through PutToFresh and would otherwise miss the mem view.
            void UpdateMemView(const TBarriersSst &sst);
            TBarriersDsSnapshot GetSnapshot(TActorSystem *as);
            TBarriersDsSnapshot GetIndexSnapshot();

        private:
            TString VDiskLogPrefix;
            std::unique_ptr<TMemView> MemView;

            void BuildMemView();
        };

    } // NBarriers
} // NKikimr

