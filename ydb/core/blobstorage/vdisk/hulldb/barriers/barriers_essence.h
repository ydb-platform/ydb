#pragma once

#include "defs.h"
#include "hullds_gcessence_defs.h"
#include "barriers_public.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap.h>

namespace NKikimr {

    namespace NBarriers {
        class TBarriersDsSnapshot;
    } // NBarriers

namespace NGcOpt {

    //////////////////////////////////////////////////////////////////////////////////////////
    // TBarriersEssence
    //////////////////////////////////////////////////////////////////////////////////////////
    class TBarriersEssence : public TThrRefBase, TNonCopyable {
    public: // public for unit tests, actually private
        TBarriersEssence(NBarriers::TMemViewSnap memViewSnap, const TBlobStorageGroupInfo::TTopology* top, const TVDiskIdShort& vDisk);

        // Builds TBarriersEssence from TBarriersDsSnapshot,
        // can work asynchronously
        static TIntrusivePtr<TBarriersEssence> Create(const THullCtxPtr &hullCtx,
                const NBarriers::TBarriersDsSnapshot &snapshot);
        static TIntrusivePtr<TBarriersEssence> Create(const THullCtxPtr &hullCtx,
                const NBarriers::TBarriersDsSnapshot &snapshot,
                ui64 firstTabletId, // 0 -- first tabletId
                ui64 lastTabletId,  // Max<ui64>() -- last tabletId
                int debugLevel);    // 0 -- by default

        NGc::TKeepStatus Keep(const TKeyLogoBlob &key,
                              const TMemRecLogoBlob &memRec,
                              ui32 recsMerged,
                              bool allowKeepFlags,
                              bool allowGarbageCollection) const {
            const TIngress ingress = memRec.GetIngress();
            Y_DEBUG_ABORT_UNLESS(recsMerged >= 1);
            return KeepLogoBlob(key.LogoBlobID(), ingress, recsMerged, allowKeepFlags, allowGarbageCollection);
        }

        NGc::TKeepStatus Keep(const TKeyBlock& /*key*/,
                              const TMemRecBlock& /*memRec*/,
                              ui32 /*recsMerged*/,
                              bool /*allowKeepFlags*/,
                              bool /*allowGarbageCollection*/) const {
            // NOTE: We never delete block records, we only merge them. Merge rules are
            //       very simple, i.e. last block wins. As a result, after full merge
            //       blocks db size is equal to number of tablets on this vdisk.
            return NGc::TKeepStatus(true);
        }

        NGc::TKeepStatus Keep(const TKeyBarrier& key,
                              const TMemRecBarrier& /*memRec*/,
                              ui32 /*recsMerged*/,
                              bool /*allowKeepFlags*/,
                              bool /*allowGarbageCollection*/) const {
            return KeepBarrier(key);
        }

        void FindBarrier(
                ui64 tabletId,
                ui8 channel,
                TMaybe<NBarriers::TCurrentBarrier> &soft,
                TMaybe<NBarriers::TCurrentBarrier> &hard) const;

        void Output(IOutputStream &str) const;
        TString ToString() const;
    private:
        NBarriers::TMemViewSnap MemViewSnap;
        std::unique_ptr<NGc::TBuildStat> BuildStat;
        const TIngress::EMode IngressMode;
        const TBlobStorageGroupInfo::TTopology* Top;
        const TVDiskIdShort& VDisk;

        NGc::TKeepStatus KeepBarrier(const TKeyBarrier &key) const;
        NGc::TKeepStatus KeepLogoBlob(const TLogoBlobID &id,
                                      const TIngress &ingress,
                                      const ui32 recsMerged,
                                      const bool allowKeepFlags,
                                      bool allowGarbageCollection) const;
    };

} // NGcOpt
} // NKikimr
