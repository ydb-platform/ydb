#pragma once

#include "defs.h"
#include "hullds_idxsnap.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexSnapshot::TForwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevelIndexSnapshot<TKey, TMemRec>::TForwardIterator
        : public TGenericForwardIterator<TKey, TFreshForwardIterator, TSliceForwardIterator>
    {
        typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
        typedef TGenericForwardIterator<TKey, TFreshForwardIterator, TSliceForwardIterator> TBase;

        TForwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : TBase(hullCtx, &levelSnap->FreshSnap, &levelSnap->SliceSnap)
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
    // TLevelIndexSnapshot::TBackwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevelIndexSnapshot<TKey, TMemRec>::TBackwardIterator
        : public TGenericBackwardIterator<TKey, TFreshBackwardIterator, TSliceBackwardIterator>
    {
        typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
        typedef TGenericBackwardIterator<TKey, TFreshBackwardIterator, TSliceBackwardIterator> TBase;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : TBase(hullCtx, &levelSnap->FreshSnap, &levelSnap->SliceSnap)
        {}

        using TBase::PutToMerger;
        using TBase::Prev;
        using TBase::Valid;
        using TBase::GetCurKey;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexSnapshot::TIndexBaseIterator
    ////////////////////////////////////////////////////////////////////////////
    template <typename TKey, typename TMemRec, bool IsForward>
    class TIndexBaseIterator {
    protected:
        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKey, TMemRec>;

        using TIterator = std::conditional_t<IsForward,
                            typename TLevelIndexSnapshot<TKey, TMemRec>::TForwardIterator,
                            typename TLevelIndexSnapshot<TKey, TMemRec>::TBackwardIterator>;

        TIndexRecordMerger Merger;
        TIterator It;
        THeapIterator<TKey, TMemRec, IsForward> HeapIt;
        std::optional<TKey> CurrentKey;

        void MergeAndAdvance() {
            Merger.Clear();
            if (HeapIt.Valid()) {
                CurrentKey.emplace(HeapIt.GetCurKey());
                HeapIt.PutToMergerAndAdvance(&Merger);
                Merger.Finish();
            } else {
                CurrentKey.reset();
            }
        }

    public:
        TIndexBaseIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot<TKey, TMemRec> *levelSnap)
            : Merger(hullCtx->VCtx->Top->GType)
            , It(hullCtx, levelSnap)
            , HeapIt(&It)
        {}

        bool Valid() const {
            return CurrentKey.has_value();
        }

        void SeekToFirst() {
            HeapIt.SeekToFirst();
            MergeAndAdvance();
        }

        void Seek(const TKey& key) {
            HeapIt.Seek(key);
            MergeAndAdvance();
        }

        TKey GetCurKey() const {
            return *CurrentKey;
        }

        const TMemRec &GetMemRec() const {
            return Merger.GetMemRec();
        }
    };
    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexSnapshot::TIndexForwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexSnapshot<TKey, TMemRec>::TIndexForwardIterator :
        public TIndexBaseIterator<TKey, TMemRec, true> {

    public:
        using TBase = TIndexBaseIterator<TKey, TMemRec, true>;

        TIndexForwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : TBase(hullCtx, levelSnap)
        {}

        using TBase::Valid;
        using TBase::SeekToFirst;
        using TBase::Seek;
        using TBase::GetCurKey;
        using TBase::GetMemRec;

        void Next() {
            MergeAndAdvance();
        }

    private:
        using TBase::MergeAndAdvance;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexSnapshot::TIndexBackwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexSnapshot<TKey, TMemRec>::TIndexBackwardIterator :
        public TIndexBaseIterator<TKey, TMemRec, false> {

    public:
        using TBase = TIndexBaseIterator<TKey, TMemRec, false>;

        TIndexBackwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : TBase(hullCtx, levelSnap)
        {}

        using TBase::Valid;
        using TBase::Seek;
        using TBase::GetCurKey;
        using TBase::GetMemRec;

        void Prev() {
            MergeAndAdvance();
        }

    private:
        using TBase::MergeAndAdvance;
    };
} // NKikimr
