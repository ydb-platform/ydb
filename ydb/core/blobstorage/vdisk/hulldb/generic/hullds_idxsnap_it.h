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
    // TLevelIndexSnapshot::TIndexForwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexSnapshot<TKey, TMemRec>::TIndexForwardIterator {
    private:
        using TForwardIterator = typename TLevelIndexSnapshot<TKey, TMemRec>::TForwardIterator;
        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKey, TMemRec>;

        TIndexRecordMerger Merger;
        TForwardIterator It;
        THeapIterator<TKey, TMemRec, true> HeapIt;
        std::optional<TKey> CurrentKey;

        void MergeAndAdvance() {
            Merger.Clear();
            if (HeapIt.Valid()) {
                CurrentKey.emplace(HeapIt.GetCurKey());
                HeapIt.PutToMergerAndAdvance(&Merger);
                Merger.Finish();
            } else{
                CurrentKey.reset();
            }
        }

    public:
        TIndexForwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : Merger(hullCtx->VCtx->Top->GType)
            , It(hullCtx, levelSnap)
        {
            It.PutToHeap(HeapIt);
        }

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

        void Next() {
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
    // TLevelIndexSnapshot::TIndexBackwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexSnapshot<TKey, TMemRec>::TIndexBackwardIterator {
    private:
        using TBackwardIterator = typename TLevelIndexSnapshot<TKey, TMemRec>::TBackwardIterator;
        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKey, TMemRec>;

        TIndexRecordMerger Merger;
        TBackwardIterator It;
        THeapIterator<TKey, TMemRec, false> HeapIt;
        std::optional<TKey> CurrentKey;

        void MergeAndAdvance() {
            Merger.Clear();
            if (HeapIt.Valid()) {
                CurrentKey.emplace(HeapIt.GetCurKey());
                HeapIt.PutToMergerAndAdvance(&Merger);
                Merger.Finish();
            } else{
                CurrentKey.reset();
            }
        }

    public:
        TIndexBackwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : Merger(hullCtx->VCtx->Top->GType)
            , It(hullCtx, levelSnap)
        {
            It.PutToHeap(HeapIt);
        }

        bool Valid() const {
            return CurrentKey.has_value();
        }

        void Seek(const TKey& key) {
            HeapIt.Seek(key);
            MergeAndAdvance();
        }

        void Prev() {
            MergeAndAdvance();
        }

        TKey GetCurKey() const {
            return *CurrentKey;
        }

        const TMemRec &GetMemRec() const {
            return Merger.GetMemRec();
        }
    };
} // NKikimr

