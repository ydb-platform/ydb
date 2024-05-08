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

    struct TKeepFlagStat {
        bool Needed = false;

        TKeepFlagStat() = default;

        template<typename TKey, typename TMemRec>
        TKeepFlagStat(const TRecordMergerBase<TKey, TMemRec>& subs, const TRecordMergerBase<TKey, TMemRec>& whole)
            : Needed(subs.GetNumDoNotKeepFlags() == whole.GetNumDoNotKeepFlags() && // DoNotKeep flag only in this record
                     subs.GetNumKeepFlags() < whole.GetNumKeepFlags()) // and Keep flag somewhere else
        {
            // Needed is set to true when we are going to compact this record, but this is the only metadata record that
            // contains DoNotKeep flag for the blob; in this case we have to keep the record without any data to prevent
            // DoNotKeep from vanishing; this flag is used only when the blob is deletable (i.e. has no flags at all, or
            // has both Keep and DoNotKeep, and also it is beyond the soft barrier)
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////
    // TBarriersEssence
    //////////////////////////////////////////////////////////////////////////////////////////
    class TBarriersEssence : public TThrRefBase, TNonCopyable {
    public: // public for unit tests, actually private
        TBarriersEssence(NBarriers::TMemViewSnap memViewSnap, TBlobStorageGroupType gtype);

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
                              TKeepFlagStat keepFlagStat,
                              bool allowKeepFlags,
                              bool allowGarbageCollection) const {
            return KeepLogoBlob(key.LogoBlobID(), memRec.GetIngress(), keepFlagStat, allowKeepFlags, allowGarbageCollection);
        }

        NGc::TKeepStatus Keep(const TKeyBlock& /*key*/,
                              const TMemRecBlock& /*memRec*/,
                              TKeepFlagStat /*keepFlagStat*/,
                              bool /*allowKeepFlags*/,
                              bool /*allowGarbageCollection*/) const {
            // NOTE: We never delete block records, we only merge them. Merge rules are
            //       very simple, i.e. last block wins. As a result, after full merge
            //       blocks db size is equal to number of tablets on this vdisk.
            return NGc::TKeepStatus(true);
        }

        NGc::TKeepStatus Keep(const TKeyBarrier& key,
                              const TMemRecBarrier& /*memRec*/,
                              TKeepFlagStat /*keepFlagStat*/,
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

        NGc::TKeepStatus KeepBarrier(const TKeyBarrier &key) const;
        NGc::TKeepStatus KeepLogoBlob(const TLogoBlobID &id,
                                      const TIngress &ingress,
                                      TKeepFlagStat keepFlagStat,
                                      const bool allowKeepFlags,
                                      bool allowGarbageCollection) const;
    };

} // NGcOpt
} // NKikimr
