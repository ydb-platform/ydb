#pragma once

#include "defs.h"
#include "hullds_sstslice_it.h"

namespace NKikimr {

    template <class TKey, class TMemRec>
    class TLevelIndex;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelIndexSnapshot
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexSnapshot {
    protected:
        typedef ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec> TLevelSliceSnapshot;
        typedef ::NKikimr::TLevelSlice<TKey, TMemRec> TLevelSlice;
        typedef TIntrusivePtr<TLevelSlice> TLevelSlicePtr;
        typedef ::NKikimr::TFreshData<TKey, TMemRec> TFreshData;
        typedef ::NKikimr::TFreshDataSnapshot<TKey, TMemRec> TFreshDataSnapshot;
        typedef typename TFreshDataSnapshot::TForwardIterator TFreshForwardIterator;
        typedef typename TFreshDataSnapshot::TBackwardIterator TFreshBackwardIterator;
        typedef typename TLevelSliceSnapshot::TForwardIterator TSliceForwardIterator;
        typedef typename TLevelSliceSnapshot::TBackwardIterator TSliceBackwardIterator;

        friend class TLevelIndex<TKey, TMemRec>;

        // TLevelIndexSnapshot is created via TLevelIndex::GetSnapshot
        TLevelIndexSnapshot(const TLevelSlicePtr &slice, TFreshDataSnapshot &&freshSnap, ui32 level0SegsNum,
                TActorSystem *actorSystem, TIntrusivePtr<TDelayedCompactionDeleterInfo> deleterInfo)
            : SliceSnap(slice, level0SegsNum)
            , FreshSnap(std::move(freshSnap))
            , Notifier(actorSystem
                    ? new TDelayedCompactionDeleterNotifier(actorSystem, std::move(deleterInfo))
                    : nullptr)
        {}

    public:
        TLevelIndexSnapshot(const TLevelIndexSnapshot &) = default;
        TLevelIndexSnapshot(TLevelIndexSnapshot &&) = default;

        void Destroy() {
            SliceSnap.Destroy();
            FreshSnap.Destroy();
            Notifier.Drop();
        }

        void Output(IOutputStream &str) const {
            SliceSnap.Output(str);
        }

        struct TForwardIterator;
        struct TBackwardIterator;

        class TIndexForwardIterator;
        class TIndexBackwardIterator;

        TLevelSliceSnapshot SliceSnap;
        TFreshDataSnapshot FreshSnap;

    private:
        TIntrusivePtr<TDelayedCompactionDeleterNotifier> Notifier;
    };

    extern template class TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier>;
    extern template class TLevelIndexSnapshot<TKeyBlock, TMemRecBlock>;

} // NKikimr
