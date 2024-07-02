#pragma once

#include "hullds_sstvec.h"
#include "hullds_sst_it.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TOrderedLevelSegments::TReadIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TOrderedLevelSegments<TKey, TMemRec>::TReadIterator {
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef typename TLevelSegment::TRec TRec;
        typedef typename TLevelSegment::TMemIterator TSegIt;
        typedef typename TSegments::const_iterator TCrossSegIt;

        using TContType = TOrderedLevelSegments;

        const TOrderedLevelSegments *All;
        TCrossSegIt CrossSegIt;
        TSegIt CurSegIt;

        TReadIterator(const THullCtxPtr &hullCtx, const TOrderedLevelSegments *orderedLevelSegment)
            : All(orderedLevelSegment)
            , CrossSegIt()
            , CurSegIt()
        {
            Y_UNUSED(hullCtx);
            Y_ABORT_UNLESS(!All->Segments.empty());
        }

        bool Valid() const {
            return CurSegIt.Valid();
        }

        TKey GetCurKey() const {
            return CurSegIt.GetCurKey();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(CrossSegIt != All->Segments.end());
            CurSegIt.Next();
            if (!CurSegIt.Valid()) {
                ++CrossSegIt;
                if (CrossSegIt == All->Segments.end()) {
                    // dead end
                    CurSegIt = {};
                } else {
                    CurSegIt = TSegIt((*CrossSegIt).Get());
                    CurSegIt.SeekToFirst();
                }
            }
        }

        void Prev() {
            CurSegIt.Prev();
            if (!CurSegIt.Valid()) {
                if (CrossSegIt == All->Segments.begin()) {
                    // dead end
                    CurSegIt = {};
                } else {
                    --CrossSegIt;
                    CurSegIt = TSegIt((*CrossSegIt).Get());
                    CurSegIt.SeekToLast();
                }
            }
        }

        void SeekToFirst() {
            Y_ABORT_UNLESS(!All->Segments.empty());
            CrossSegIt = All->Segments.begin();
            CurSegIt = TSegIt((*CrossSegIt).Get());
            CurSegIt.SeekToFirst();
        }

        void SeekToLast() {
            Y_ABORT_UNLESS(!All->Segments.empty());
            CrossSegIt = All->Segments.end() - 1;
            CurSegIt = TSegIt((*CrossSegIt).Get());
            CurSegIt.SeekToLast();
        }

        void Seek(const TKey &key) {
            Y_ABORT_UNLESS(!All->Segments.empty());
            TCrossSegIt b = All->Segments.begin();
            TCrossSegIt e = All->Segments.end();
            CrossSegIt = ::LowerBound(b, e, key, TVectorLess());

            if (CrossSegIt == e) {
                Y_DEBUG_ABORT_UNLESS(b != e); // we can't have empty vector
                --CrossSegIt;
            } else {
                const TKey firstKey = (*CrossSegIt)->FirstKey();
                Y_DEBUG_ABORT_UNLESS(firstKey >= key);
                if (firstKey > key) {
                    if (CrossSegIt == b) {
                        // good
                    } else {
                        --CrossSegIt;
                        const TKey lastKey = (*CrossSegIt)->LastKey();
                        if (key > lastKey)
                            ++CrossSegIt;
                    }
                } else {
                    // good
                }
            }

            CurSegIt = TSegIt((*CrossSegIt).Get());
            CurSegIt.Seek(key);
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            Y_DEBUG_ABORT_UNLESS(Valid());
            CurSegIt.template PutToMerger<TRecordMerger>(merger);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            heap.Add(this);
        }

        const TLevelSegment *GetCurSstPtr() const {
            return CurSegIt.GetSstPtr();
        }

        const TRec &operator*() const {
            return CurSegIt.operator*();
        }

        const TRec *operator->() const {
            return CurSegIt.operator->();
        }

        bool operator ==(const TReadIterator &it) const {
            return CrossSegIt == it.CrossSegIt && CurSegIt == it.CurSegIt;
        }

        bool operator !=(const TReadIterator &it) const {
            return !operator == (it);
        }

        TDiskDataExtractor *GetDiskData(TDiskDataExtractor *extr) const {
            return CurSegIt.GetDiskData(extr);
        }

        const TDiskPart *GetOutbound() const {
            return CurSegIt.GetOutbound();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TOrderedLevelSegments::TSstIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TOrderedLevelSegments<TKey, TMemRec>::TSstIterator {
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef typename TOrderedLevelSegments::TSegments TSegments;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;


        const TOrderedLevelSegments *S;
        typename TSegments::const_iterator Cur;

    public:
        TSstIterator()
            : S(nullptr)
            , Cur()
        {}

        TSstIterator(const TOrderedLevelSegments *s)
            : S(s)
            , Cur()
        {}

        void SeekToFirst() {
            Y_DEBUG_ABORT_UNLESS(S);
            Cur = S->Segments.begin();
        }

        void SeekToLast() {
            Y_DEBUG_ABORT_UNLESS(S);
            Cur = S->Segments.end();
            --Cur;
        }

        void Seek(const TKey &key) {
            auto less = [] (const TLevelSegmentPtr &sst, const TKey &key) {
                return sst->FirstKey() < key;
            };
            typename TSegments::const_iterator begin = S->Segments.begin();
            typename TSegments::const_iterator end = S->Segments.end();
            Cur = ::LowerBound(begin, end, key, less);
        }

        bool Valid() const {
            return S && Cur >= S->Segments.begin() && Cur < S->Segments.end();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++Cur;
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            --Cur;
        }

        TLevelSegmentPtr Get() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return *Cur;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TUnorderedLevelSegments::TSstIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TUnorderedLevelSegments<TKey, TMemRec>::TSstIterator {
        typedef ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec> TUnorderedLevelSegments;
        typedef typename TUnorderedLevelSegments::TSegments TSegments;

        const TUnorderedLevelSegments *S;
        ui32 NumLimit;
        ui32 CurNum;
        typename TSegments::const_iterator Cur;

    public:
        TSstIterator(const TUnorderedLevelSegments *s, ui32 numLimit)
            : S(s)
            , NumLimit(numLimit)
            , CurNum(ui32(-1))
            , Cur()
        {}

        void SeekToFirst() {
            Y_DEBUG_ABORT_UNLESS(S);
            if (NumLimit > 0) {
                // for zero NumLimit we have a race of adding element into
                // empty list and calling begin() on it
                Cur = S->Segments.begin();
                CurNum = 0;
            } else {
                // invalid iterator
                CurNum = ui32(-1);
            }
        }

        bool Valid() const {
            return S && CurNum != ui32(-1) && CurNum < NumLimit;
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++CurNum;
            if (CurNum < NumLimit) {
                // update Cur only if we don't stay at the last elements,
                // otherwise we have a race
                Y_DEBUG_ABORT_UNLESS(Cur != S->Segments.end());
                ++Cur;
            }
        }

        TLevelSegmentPtr Get() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return *Cur;
        }
    };

} // NKikimr
