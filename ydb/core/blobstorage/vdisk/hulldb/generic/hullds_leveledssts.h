#pragma once

#include "defs.h"
#include "hullds_sstvec.h"

namespace NKikimr {

    template <class TKey, class TMemRec>
    class TLeveledSsts {
    private:
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
        typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;


        typedef TVector<TLevelSstPtr> TVectorType;
        TVectorType Vec;

    public:
        TLeveledSsts()
            : Vec()
        {}

        TLeveledSsts(ui32 level, const TOrderedLevelSegments &s)
            : Vec()
        {
            typename TOrderedLevelSegments::TSstIterator it(&s);
            it.SeekToFirst();
            Y_DEBUG_ABORT_UNLESS(it.Valid());
            while (it.Valid()) {
                PushBack(TLevelSstPtr(level, it.Get()));
                it.Next();
            }
        }

        void Clear() {
            Vec.clear();
        }

        void PushBack(const TLevelSstPtr &x) {
            Vec.push_back(x);
        }

        void Swap(TLeveledSsts &s) {
            Vec.swap(s.Vec);
        }

        void Sort() {
            ::Sort(Vec.begin(), Vec.end());
        }

        bool Empty() const {
            return Vec.empty();
        }

        class TIterator;
    };


    template <class TKey, class TMemRec>
    class TLeveledSsts<TKey, TMemRec>::TIterator {
    private:
        const TLeveledSsts *LeveledSsts;
        size_t Pos;

    public:
        TIterator()
            : LeveledSsts(nullptr)
            , Pos(0)
        {}

        TIterator(const TLeveledSsts *leveledSsts)
            : LeveledSsts(leveledSsts)
            , Pos(0)
        {}

        TIterator(const TIterator&) = default;
        TIterator& operator =(const TIterator&) = default;

        void SeekToFirst() {
            Pos = 0;
        }

        bool Valid() const {
            return LeveledSsts && Pos < LeveledSsts->Vec.size();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            Pos++;
        }

        TLevelSstPtr Get() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return LeveledSsts->Vec[Pos];
        }
    };

} // NKikimr
