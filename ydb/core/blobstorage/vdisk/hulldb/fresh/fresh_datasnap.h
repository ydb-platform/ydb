#pragma once

#include "defs.h"
#include "fresh_segment.h"
#include <library/cpp/threading/skip_list/skiplist.h>
#include <util/system/align.h>
#include <util/generic/set.h>


namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////
    // TFreshData forward declaration
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshData;

    /////////////////////////////////////////////////////////////////////////
    // FreshDataSnapshot
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshDataSnapshot {
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TSegForwardIterator = typename TFreshSegmentSnapshot::TForwardIterator;
        using TSegBackwardIterator = typename TFreshSegmentSnapshot::TBackwardIterator;
        friend class TFreshData<TKey, TMemRec>;

        TFreshDataSnapshot(
                TFreshSegmentSnapshot &&old,
                TFreshSegmentSnapshot &&dreg,
                TFreshSegmentSnapshot &&cur)
            : Old(std::move(old))
            , Dreg(std::move(dreg))
            , Cur(std::move(cur))
        {}

        class TForwardOldDregSegsMerger;    // forward iterator, merges Old and Dreg
        class TBackwardCurDregSegsMerger;   // backward iterator, merges Cur and Dreg

    public:
        TFreshDataSnapshot(const TFreshDataSnapshot &snap) = default;
        TFreshDataSnapshot &operator=(const TFreshDataSnapshot &) = default;
        TFreshDataSnapshot(TFreshDataSnapshot &&) = default;
        TFreshDataSnapshot &operator=(TFreshDataSnapshot &&) = default;

        void Destroy() {
            Old.Destroy();
            Dreg.Destroy();
            Cur.Destroy();
        }

        TFreshSegmentSnapshot Old;
        TFreshSegmentSnapshot Dreg;
        TFreshSegmentSnapshot Cur;

        class TForwardIterator;     // forward iterator, merges TForwardOldDregSegsMerger and Cur
        class TBackwardIterator;    // backward iterator, merges TBackwardCurDregSegsMerger and Old
    };

    extern template class TFreshDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshDataSnapshot<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshDataSnapshot<TKeyBlock, TMemRecBlock>;

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshDataSnapshot::TForwardOldDregSegsMerger
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshDataSnapshot<TKey, TMemRec>::TForwardOldDregSegsMerger
        : public TGenericForwardIterator<TKey, TSegForwardIterator, TSegForwardIterator>
    {
    public:
        typedef ::NKikimr::TGenericForwardIterator<TKey, TSegForwardIterator, TSegForwardIterator> TBase;
        typedef ::NKikimr::TFreshDataSnapshot<TKey, TMemRec> TFreshDataSnapshot;
        typedef TFreshDataSnapshot TContType;

        TForwardOldDregSegsMerger(const THullCtxPtr &hullCtx, const TContType *freshData)
            : TBase(hullCtx, &freshData->Old, &freshData->Dreg)
        {}

        using TBase::PutToMerger;
        using TBase::Next;
        using TBase::Valid;
        using TBase::Seek;
        using TBase::PutToHeap;
    };


    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshDataSnapshot::TBackwardCurDregSegsMerger
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshDataSnapshot<TKey, TMemRec>::TBackwardCurDregSegsMerger
        : public TGenericBackwardIterator<TKey, TSegBackwardIterator, TSegBackwardIterator>
    {
    public:
        typedef ::NKikimr::TGenericBackwardIterator<TKey, TSegBackwardIterator, TSegBackwardIterator> TBase;
        typedef ::NKikimr::TFreshDataSnapshot<TKey, TMemRec> TFreshDataSnapshot;
        typedef TFreshDataSnapshot TContType;

        TBackwardCurDregSegsMerger(const THullCtxPtr &hullCtx, const TContType *freshData)
            : TBase(hullCtx, &freshData->Cur, &freshData->Dreg)
        {}

        using TBase::PutToMerger;
        using TBase::Prev;
        using TBase::Valid;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshDataSnapshot Forward Iterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshDataSnapshot<TKey, TMemRec>::TForwardIterator
        : public TGenericForwardIterator<TKey, TForwardOldDregSegsMerger, TSegForwardIterator>
    {
    public:
        typedef ::NKikimr::TGenericForwardIterator<TKey, TForwardOldDregSegsMerger, TSegForwardIterator> TBase;
        typedef ::NKikimr::TFreshDataSnapshot<TKey, TMemRec> TFreshDataSnapshot;
        typedef TFreshDataSnapshot TContType;

        TForwardIterator(const THullCtxPtr &hullCtx, const TContType *freshData)
            : TBase(hullCtx, freshData, &freshData->Cur)
        {}

        using TBase::PutToMerger;
        using TBase::Next;
        using TBase::Valid;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshDataSnapshot Backward Iterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshDataSnapshot<TKey, TMemRec>::TBackwardIterator
        : public TGenericBackwardIterator<TKey, TBackwardCurDregSegsMerger, TSegBackwardIterator>
    {
    public:
        typedef ::NKikimr::TGenericBackwardIterator<TKey, TBackwardCurDregSegsMerger, TSegBackwardIterator> TBase;
        typedef ::NKikimr::TFreshDataSnapshot<TKey, TMemRec> TFreshDataSnapshot;
        typedef TFreshDataSnapshot TContType;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TContType *freshData)
            : TBase(hullCtx, freshData, &freshData->Old)
        {}

        using TBase::PutToMerger;
        using TBase::Prev;
        using TBase::Valid;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

} // NKikimr
