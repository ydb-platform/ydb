#pragma once

#include "defs.h"
#include "hullds_idxsnap.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>

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

        void Merge() {
            Merger.Clear();
            if (It.Valid()) {
                It.PutToMerger(&Merger);
                Merger.Finish();
            }
        }

    public:
        TIndexForwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : Merger(hullCtx->VCtx->Top->GType)
            , It(hullCtx, levelSnap)
        {}

        bool Valid() const {
            return It.Valid();
        }

        void SeekToFirst() {
            It.SeekToFirst();
            Merge();
        }

        void Seek(const TKey& key) {
            It.Seek(key);
            Merge();
        }

        void Next() {
            It.Next();
            Merge();
        }

        TKey GetCurKey() const {
            return It.GetCurKey();
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

        void Merge() {
            Merger.Clear();
            if (It.Valid()) {
                It.PutToMerger(&Merger);
                Merger.Finish();
            }
        }

    public:
        TIndexBackwardIterator(const THullCtxPtr &hullCtx, const TLevelIndexSnapshot *levelSnap)
            : Merger(hullCtx->VCtx->Top->GType)
            , It(hullCtx, levelSnap)
        {}

        bool Valid() const {
            return It.Valid();
        }

        void Seek(const TKey& key) {
            It.Seek(key);
            Merge();
        }

        void Prev() {
            It.Prev();
            Merge();
        }

        TKey GetCurKey() const {
            return It.GetCurKey();
        }

        const TMemRec &GetMemRec() const {
            return Merger.GetMemRec();
        }
    };
} // NKikimr

