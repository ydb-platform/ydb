#include "barriers_essence.h"


namespace NKikimr {
    namespace NGcOpt {

        TBarriersEssence::TBarriersEssence(NBarriers::TMemViewSnap memViewSnap, TBlobStorageGroupType gtype)
            : MemViewSnap(std::move(memViewSnap))
            , BuildStat(std::make_unique<NGc::TBuildStat>())
            , IngressMode(TIngress::IngressMode(gtype))
        {}

        TIntrusivePtr<TBarriersEssence> TBarriersEssence::Create(const THullCtxPtr &hullCtx,
                const NBarriers::TBarriersDsSnapshot &snapshot)
        {
            return MakeIntrusive<TBarriersEssence>(snapshot.GetMemViewSnap(), hullCtx->VCtx->Top->GType);
        }

        TIntrusivePtr<TBarriersEssence> TBarriersEssence::Create(const THullCtxPtr &hullCtx,
                const NBarriers::TBarriersDsSnapshot &snapshot,
                ui64 /*firstTabletId*/,
                ui64 /*lastTabletId*/,
                int /*debugLevel*/)
        {
            return Create(hullCtx, snapshot);
        }

        void TBarriersEssence::FindBarrier(
                ui64 tabletId,
                ui8 channel,
                TMaybe<NBarriers::TCurrentBarrier> &soft,
                TMaybe<NBarriers::TCurrentBarrier> &hard) const
        {
            MemViewSnap.GetBarrier(tabletId, channel, soft, hard);
        }

        void TBarriersEssence::Output(IOutputStream &str) const {
            MemViewSnap.Output(str);
        }

        TString TBarriersEssence::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        NGc::TKeepStatus TBarriersEssence::KeepBarrier(const TKeyBarrier &key) const {
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;
            MemViewSnap.GetBarrier(key.TabletId, key.Channel, soft, hard);

            if (key.Hard) {
                return hard.Empty() ||
                    (std::make_tuple(key.Gen, key.GenCounter) >= std::make_tuple(hard->Gen, hard->GenCounter));
            } else {
                return soft.Empty() ||
                    (std::make_tuple(key.Gen, key.GenCounter) >= std::make_tuple(soft->Gen, soft->GenCounter));
            }
        }

        NGc::TKeepStatus TBarriersEssence::KeepLogoBlob(const TLogoBlobID &id,
                                      const TIngress &ingress,
                                      TKeepFlagStat keepFlagStat,
                                      const bool allowKeepFlags,
                                      bool allowGarbageCollection) const
        {
            if (!allowGarbageCollection) {
                return {true};
            }

            // extract gen and step
            const ui32 gen = id.Generation();
            const ui32 step = id.Step();

            // find current barrier for these tablet id and channel
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;
            MemViewSnap.GetBarrier(id.TabletID(), id.Channel(), soft, hard);

            // check the hard barrier
            const bool keepByHardBarrier = hard.Empty() ||
                std::make_tuple(gen, step) > std::make_tuple(hard->CollectGen, hard->CollectStep);

            // check the soft barrier
            const bool keepBySoftBarrier = soft.Empty() ||
                std::make_tuple(gen, step) > std::make_tuple(soft->CollectGen, soft->CollectStep);

            // flags says us to keep the record?
            const bool keepByFlags = ingress.KeepUnconditionally(IngressMode);

            // check if we have to keep data associated with this blob
            const bool keepData = (keepBySoftBarrier || keepByFlags) && keepByHardBarrier;

            // check if we have to keep index data; when item is stored in multiple SSTables, we can't just drop one
            // index and keep others as this index may contain Keep flag vital for blob consistency -- in case when
            // we drop such record, we will lose keep flag and item will be occasionally on next compaction; anyway,
            // if hard barrier tells us to drop this item, we drop it unconditinally
            const bool spreadFactor = allowKeepFlags && keepFlagStat.Needed;
            const bool keepIndex = (keepData || spreadFactor) && keepByHardBarrier;

            return NGc::TKeepStatus(keepIndex, keepData, keepBySoftBarrier && keepByHardBarrier);
        }

    } // NGcOpt
} // NKikimr

