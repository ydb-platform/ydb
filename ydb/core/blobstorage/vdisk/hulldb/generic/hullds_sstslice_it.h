#pragma once

#include "defs.h"
#include "hullds_sstvec_it.h"
#include "hullds_sstslice.h"
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_data.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSlice::TSortedLevelsIter
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSlice<TKey, TMemRec>::TSortedLevelsIter {
        using TLevelsXIterator = typename TSortedLevels::const_iterator;

        const TSortedLevels *Levels = nullptr;
        TLevelsXIterator CurLevelIt; // iterates via levels starting from 1
        ui32 CurLevelNum = 0;

    public:
        struct TSortedLevelRef {
            const ui32 Level;
            const TSortedLevel &SortedLevel;
        };

        TSortedLevelsIter(const TLevelSlice *slice)
            : Levels(&slice->SortedLevels)
            , CurLevelIt()
            , CurLevelNum(0)
        {}

        void SeekToFirst() {
            CurLevelIt = Levels->begin();
            CurLevelNum = 1;
        }

        void SeekToLast() {
            if (Levels->empty()) {
                CurLevelIt = Levels->end();
                CurLevelNum = 0;
            } else {
                CurLevelIt = Levels->end() - 1;
                CurLevelNum = Levels->size();
            }
        }

        bool Valid() const {
            return Levels && CurLevelIt >= Levels->begin() && CurLevelIt < Levels->end();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++CurLevelIt;
            ++CurLevelNum;
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            --CurLevelIt;
            --CurLevelNum;
        }

        TSortedLevelRef Get() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return { CurLevelNum, *CurLevelIt };
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TLevelSlice::TSortedLevelsSSTIter
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSlice<TKey, TMemRec>::TSortedLevelsSSTIter {
        typedef typename TSortedLevels::const_iterator TLevelsXIterator;
        typedef typename TOrderedLevelSegments::TSstIterator TOrderedLevelSegmentsSstIterator;

        const TSortedLevels *Levels;
        TLevelsXIterator CurLevelIt; // iterates via levels starting from 1
        ui32 CurLevelNum;
        TOrderedLevelSegmentsSstIterator IntraLevelIt; // iterates inside level

    public:
        TSortedLevelsSSTIter(const TSortedLevels *levels)
            : Levels(levels)
            , CurLevelIt()
            , CurLevelNum(0)
            , IntraLevelIt()
        {}

        void SeekToFirst() {
            CurLevelIt = Levels->begin();
            CurLevelNum = 1;
            PositionItraLevelIt();
        }

        bool Valid() const {
            return Levels && CurLevelIt >= Levels->begin() && CurLevelIt < Levels->end() && IntraLevelIt.Valid();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            IntraLevelIt.Next();
            if (!IntraLevelIt.Valid()) {
                ++CurLevelIt;
                ++CurLevelNum;
                PositionItraLevelIt();
            }
        }

        TLevelSstPtr Get() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return TLevelSstPtr(CurLevelNum, IntraLevelIt.Get());
        }

        template <class Heap>
        void PutToHeap(Heap& heap) {
            heap.Add(this);
        }

    private:
        void PositionItraLevelIt() {
            while (CurLevelIt != Levels->end()) {
                if (CurLevelIt->Empty()) {
                    ++CurLevelIt;
                    ++CurLevelNum;
                } else {
                    IntraLevelIt = TOrderedLevelSegmentsSstIterator(CurLevelIt->Segs.Get());
                    IntraLevelIt.SeekToFirst();
                    break;
                }
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSlice::TSstIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSlice<TKey, TMemRec>::TSstIterator {
        typedef ::NKikimr::TLevelSlice<TKey, TMemRec> TLevelSlice;

        typename ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec>::TSstIterator Level0It;
        typename TLevelSlice::TSortedLevelsSSTIter SortedLevelsIt;

    public:
        TSstIterator(const TLevelSlice *slice, ui32 level0NumLimit)
            : Level0It(slice->Level0.Segs.Get(), level0NumLimit)
            , SortedLevelsIt(&(slice->SortedLevels))
        {}

        void SeekToFirst() {
            Level0It.SeekToFirst();
            SortedLevelsIt.SeekToFirst();
        }

        bool Valid() const {
            return Level0It.Valid() || SortedLevelsIt.Valid();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            if (Level0It.Valid())
                Level0It.Next();
            else
                SortedLevelsIt.Next();
        }

        TLevelSstPtr Get() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            if (Level0It.Valid())
                return TLevelSstPtr(0, Level0It.Get());
            else
                return SortedLevelsIt.Get();
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TLevelSliceSnapshot::TSortedLevelsIter
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSliceSnapshot<TKey, TMemRec>::TSortedLevelsIter : public ::NKikimr::TLevelSlice<TKey, TMemRec>::TSortedLevelsIter {
        typedef ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec> TLevelSliceSnapshot;
        typedef typename ::NKikimr::TLevelSlice<TKey, TMemRec>::TSortedLevelsIter TBase;

    public:
        TSortedLevelsIter(const TLevelSliceSnapshot *snap)
            : TBase(snap->Slice.Get())
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TLevelSliceSnapshot::TSstIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSliceSnapshot<TKey, TMemRec>::TSstIterator : public ::NKikimr::TLevelSlice<TKey, TMemRec>::TSstIterator {
        typedef ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec> TLevelSliceSnapshot;
        typedef typename ::NKikimr::TLevelSlice<TKey, TMemRec>::TSstIterator TBase;

    public:
        TSstIterator(const TLevelSliceSnapshot *snap)
            : TBase(snap->Slice.Get(), snap->Level0SegsNum)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // NHullPrivate
    // Common private routines
    ////////////////////////////////////////////////////////////////////////////
    namespace NHullPrivate {

        // converter for input parameters
        template <class TKey, class TMemRec>
        static inline TVector<TOrderedLevelSegments<TKey, TMemRec>*>
        Convert(const TVector<TIntrusivePtr<TOrderedLevelSegments<TKey, TMemRec>>> &vec) {
            TVector<TOrderedLevelSegments<TKey, TMemRec>*> result;
            result.reserve(vec.size());
            for (const auto &x : vec) {
                result.push_back(x.Get());
            }
            return result;
        }

        // dump all data accessible by this iterator
        template <class TSegments>
        void DumpAll(IOutputStream &str, const TSegments &segs) {
            for (const auto &x : segs) {
                str << "=== Ordered Level ===\n";
                x->DumpAll(str);
            }
        }

    } // NHullPrivate

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSlice::TForwardIterator -- forward iterator over ordered level segment vectors
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSlice<TKey, TMemRec>::TForwardIterator {
        using TOrderedLevelSegments = ::NKikimr::TOrderedLevelSegments<TKey, TMemRec>;
        using TOrderedLevelSegmentsPtr = TIntrusivePtr<TOrderedLevelSegments>;
        using TReadIterator = typename TOrderedLevelSegments::TReadIterator;
        using TNWayIterator = TGenericNWayForwardIterator<TKey, TReadIterator>;

        TVector<TOrderedLevelSegmentsPtr> Segs; // just to save them
        TNWayIterator It;

    public:
        TForwardIterator(const THullCtxPtr &hullCtx, const TVector<TOrderedLevelSegmentsPtr> &vec)
            : Segs(vec)
            , It(hullCtx, NHullPrivate::Convert(Segs))
        {}
        TForwardIterator(TForwardIterator &&) = default;
        TForwardIterator& operator=(TForwardIterator &&) = default;
        TForwardIterator(const TForwardIterator &) = default;
        TForwardIterator &operator=(TForwardIterator &) = default;

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            It.template PutToMerger<TRecordMerger>(merger);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            It.PutToHeap(heap);
        }

        bool Valid() const {
            return It.Valid();
        }

        void Next() {
            It.Next();
        }

        TKey GetCurKey() const {
            return It.GetCurKey();
        }

        void Seek(const TKey &key) {
            It.Seek(key);
        }

        void SeekToFirst() {
            It.SeekToFirst();
        }

        // dump all data accessible by this iterator
        void DumpAll(IOutputStream &str) const {
            NHullPrivate::DumpAll(str, Segs);
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSlice::TBackwardIterator -- backward iterator over ordered level segment vectors
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSlice<TKey, TMemRec>::TBackwardIterator {
        using TOrderedLevelSegments = ::NKikimr::TOrderedLevelSegments<TKey, TMemRec>;
        using TOrderedLevelSegmentsPtr = TIntrusivePtr<TOrderedLevelSegments>;
        using TReadIterator = typename TOrderedLevelSegments::TReadIterator;
        using TNWayIterator = TGenericNWayBackwardIterator<TKey, TReadIterator>;

        TVector<TOrderedLevelSegmentsPtr> Segs; // just to save them
        TNWayIterator It;

    public:
        TBackwardIterator(const THullCtxPtr &hullCtx, const TVector<TOrderedLevelSegmentsPtr> &vec)
            : Segs(vec)
            , It(hullCtx, NHullPrivate::Convert(Segs))
        {}

        TBackwardIterator(TBackwardIterator &&) = default;
        TBackwardIterator& operator=(TBackwardIterator &&) = default;
        TBackwardIterator(const TBackwardIterator &) = default;
        TBackwardIterator &operator=(TBackwardIterator &) = default;

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            It.template PutToMerger<TRecordMerger>(merger);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            It.PutToHeap(heap);
        }

        bool Valid() const {
            return It.Valid();
        }

        void Prev() {
            It.Prev();
        }

        TKey GetCurKey() const {
            return It.GetCurKey();
        }

        void Seek(const TKey &key) {
            It.Seek(key);
        }

        // dump all data accessible by this iterator
        void DumpAll(IOutputStream &str) const {
            NHullPrivate::DumpAll(str, Segs);
        }
    };
    ////////////////////////////////////////////////////////////////////////////
    // NHullPrivate
    // Common private routines
    ////////////////////////////////////////////////////////////////////////////
    namespace NHullPrivate {

        template <class TKey, class TMemRec>
        static inline
        TVector<TIntrusivePtr<TOrderedLevelSegments<TKey, TMemRec>>>
        Convert(const TLevelSliceSnapshot<TKey, TMemRec> *sliceSnap) {
            using TOrderedLevelSegments = ::NKikimr::TOrderedLevelSegments<TKey, TMemRec>;
            using TOrderedLevelSegmentsPtr = TIntrusivePtr<TOrderedLevelSegments>;

            TVector<TOrderedLevelSegmentsPtr> result;
            result.reserve(sliceSnap->GetLevel0SstsNum() + sliceSnap->GetLevelXNumber());
            // cycle through LevelO segments
            typename TUnorderedLevelSegments<TKey, TMemRec>::TSstIterator l0it = sliceSnap->GetLevel0SstIterator();
            l0it.SeekToFirst();
            while (l0it.Valid()) {
                TOrderedLevelSegmentsPtr segsPtr(new TOrderedLevelSegments(l0it.Get()));
                result.push_back(segsPtr);
                l0it.Next();
            }

            // cycle through multiple LevelX instances
            for (ui32 i = 0, n = sliceSnap->GetLevelXNumber(); i < n; i++) {
                result.push_back(sliceSnap->GetLevelXRef(i).Segs);
            }

            return result;
        }

    } // NHullPrivate


    ////////////////////////////////////////////////////////////////////////////
    // TLevelSliceSnapshot::TForwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSliceSnapshot<TKey, TMemRec>::TForwardIterator
            : public ::NKikimr::TLevelSlice<TKey, TMemRec>::TForwardIterator
    {
    public:
        using TLevelSlice = ::NKikimr::TLevelSlice<TKey, TMemRec>;
        using TLevelSliceForwardIterator = typename TLevelSlice::TForwardIterator;
        using TBase = TLevelSliceForwardIterator;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TContType = TLevelSliceSnapshot;

        TForwardIterator(const THullCtxPtr &hullCtx, const TLevelSliceSnapshot *sliceSnap)
            : TBase(hullCtx, NHullPrivate::Convert(sliceSnap))
        {}

        using TBase::PutToMerger;
        using TBase::Next;
        using TBase::Valid;
        using TBase::GetCurKey;
        using TBase::Seek;
        using TBase::SeekToFirst;
        using TBase::DumpAll;
        using TBase::PutToHeap;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSliceSnapshot::TBackwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSliceSnapshot<TKey, TMemRec>::TBackwardIterator
        : public ::NKikimr::TLevelSlice<TKey, TMemRec>::TBackwardIterator
    {
    public:
        using TLevelSlice = ::NKikimr::TLevelSlice<TKey, TMemRec>;
        using TLevelSliceBackwardIterator = typename TLevelSlice::TBackwardIterator;
        using TBase = TLevelSliceBackwardIterator;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TContType = TLevelSliceSnapshot;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TLevelSliceSnapshot *sliceSnap)
            : TBase(hullCtx, NHullPrivate::Convert(sliceSnap))
        {}

        using TBase::PutToMerger;
        using TBase::Prev;
        using TBase::Valid;
        using TBase::GetCurKey;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

} // NKikimr

