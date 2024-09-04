#pragma once

#include "defs.h"
#include "snap_vec.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_generic_it.h>
// FIXME
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/core/blobstorage/base/ptr.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendix -- immutable appendix to fresh segment
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendix {
    public:
        struct TRecord {
            TKey Key;
            TMemRec MemRec;

            bool operator <(const TKey &key) const {
                return Key < key;
            }

            bool operator <(const TRecord &rec) const {
                return Key < rec.Key;
            }

            bool operator ==(const TRecord &rec) const {
                return Key == rec.Key;
            }

            TRecord(const TKey &key, const TMemRec &memRec)
                : Key(key)
                , MemRec(memRec)
            {}
        };

        using TVec = TVector<TRecord>;

        TFreshAppendix(const TMemoryConsumer &memConsumer)
            : MemConsumed(memConsumer)
        {}

        TFreshAppendix(TVec &&vec, const TMemoryConsumer &memConsumer, bool sorted = false)
            : MemConsumed(memConsumer)
        {
            if (!sorted) {
                std::sort(vec.begin(), vec.end());
            }
            SortedRecs.swap(vec);
            MemConsumed.Add(sizeof(TRecord) * SortedRecs.size());
        }

        TFreshAppendix(TFreshAppendix &&) = default;
        TFreshAppendix &operator=(TFreshAppendix &&) = default;

        void Add(const TKey &key, const TMemRec &memRec) {
            Y_DEBUG_ABORT_UNLESS(SortedRecs.empty() || SortedRecs.back().Key < key);
            SortedRecs.push_back({key, memRec});
            MemConsumed.Add(sizeof(TRecord));
        }

        bool Empty() const {
            return SortedRecs.empty();
        }

        void Reserve(size_t n) {
            SortedRecs.reserve(n);
        }

        size_t GetSize() const {
            return SortedRecs.size();
        }

        ui64 SizeApproximation() const {
            return sizeof(TRecord) * SortedRecs.size();
        }

        ::NMonitoring::TDynamicCounters::TCounterPtr GetCounter() const {
            return MemConsumed.GetCounter();
        }

        static std::shared_ptr<TFreshAppendix> Merge(
            const THullCtxPtr &hullCtx,
            const TVector<std::shared_ptr<TFreshAppendix>> &v);

        class TIterator;

    private:
        TVec SortedRecs;
        TMemoryConsumerWithDropOnDestroy MemConsumed;

        using TVecIterator = typename TVec::iterator;
    };

    extern template class TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshAppendix<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshAppendix<TKeyBlock, TMemRecBlock>;

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendix::TIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendix<TKey, TMemRec>::TIterator {
    public:
        using TFreshAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;
        using TContType = TFreshAppendix;

        TIterator(const THullCtxPtr &hullCtx, TContType *apndx)
            : Apndx(apndx)
            , It()
        {
            Y_UNUSED(hullCtx);
        }

        bool Valid() const {
            return Apndx && It >= Apndx->SortedRecs.begin() && It < Apndx->SortedRecs.end();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++It;
        }

        void Prev() {
            if (It == Apndx->SortedRecs.begin())
                It = {};
            else
                --It;
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return It->Key;
        }

        TMemRec GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return It->MemRec;
        }

        void SeekToFirst() {
            It = Apndx->SortedRecs.begin();
        }

        void Seek(const TKey &key) {
            It = ::LowerBound(Apndx->SortedRecs.begin(), Apndx->SortedRecs.end(), key);
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            // because fresh appendix doesn't have data we don't care about exact circaLsn value
            const ui64 circaLsn = 0;
            merger->AddFromFresh(It->MemRec, nullptr, It->Key, circaLsn);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            heap.Add(this);
        }

    private:
        TFreshAppendix *Apndx;
        TFreshAppendix::TVecIterator It;
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendix -- merge segments
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    std::shared_ptr<TFreshAppendix<TKey, TMemRec>> TFreshAppendix<TKey, TMemRec>::Merge(
            const THullCtxPtr &hullCtx,
            const TVector<std::shared_ptr<TFreshAppendix<TKey, TMemRec>>> &v)
    {
        using TFreshAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;
        using TAppendixIterator = typename TFreshAppendix::TIterator;
        using TNWayIterator = TGenericNWayForwardIterator<TKey, TAppendixIterator>;

        TVector<TFreshAppendix*> input;
        input.reserve(v.size());
        size_t reserve = 0;
        for (auto &x : v) {
            input.push_back(x.get());
            reserve += x->GetSize();
        }
        typename TFreshAppendix::TVec vec;
        vec.reserve(reserve);
        TIndexRecordMerger<TKey, TMemRec> merger(hullCtx->VCtx->Top->GType);
        TNWayIterator it(hullCtx, input);
        it.SeekToFirst();
        while (it.Valid()) {
            it.PutToMerger(&merger);
            merger.Finish();

            vec.emplace_back(it.GetCurKey(), merger.GetMemRec());

            it.Next();
            merger.Clear();
        }

        return std::make_shared<TFreshAppendix>(std::move(vec), TMemoryConsumer(input[0]->GetCounter()), true);
    }


    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendixTreeSnap -- a snapshot of TFreshAppendixTree
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendixTree;

    template <class TKey, class TMemRec>
    class TFreshAppendixTreeSnap {
    private:
        using TAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;
        using TAppendixPtr = std::shared_ptr<TAppendix>;
        using TTree = TSTree<TAppendix, THullCtxPtr>;
        using TTreeSnap = TSTreeSnap<TAppendix, THullCtxPtr>;
        friend class TFreshAppendixTree<TKey, TMemRec>;

        TTreeSnap Snap;

        TFreshAppendixTreeSnap(const TTreeSnap &snap)
            : Snap(snap)
        {}

        class TPrivateIteratorBase;

    public:
        TFreshAppendixTreeSnap() = default;
        TFreshAppendixTreeSnap(const TFreshAppendixTreeSnap &) = default;
        TFreshAppendixTreeSnap(TFreshAppendixTreeSnap &&) = default;
        TFreshAppendixTreeSnap &operator=(const TFreshAppendixTreeSnap &) = default;
        TFreshAppendixTreeSnap &operator=(TFreshAppendixTreeSnap &&) = default;

        void Destroy() {
            Snap.Destroy();
        }

        class TForwardIterator;
        class TBackwardIterator;
        // iterates via TAppendixPtr instead of <TKey, TMemRec>
        class TAppendixIterator : public TTreeSnap::TIterator {
        public:
            using TBase = typename TTreeSnap::TIterator;
            TAppendixIterator(const TFreshAppendixTreeSnap *snap)
                : TBase(&snap->Snap)
            {}
        };
    };

    extern template class TFreshAppendixTreeSnap<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshAppendixTreeSnap<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshAppendixTreeSnap<TKeyBlock, TMemRecBlock>;

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendixTree -- several TFreshAppendix
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendixTree {
    private:
        using TAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;
        using TAppendixPtr = std::shared_ptr<TAppendix>;
        using TTree = TSTree<TAppendix, THullCtxPtr>;
        using TTreeSnap = TSTreeSnap<TAppendix, THullCtxPtr>;

        TTree Tree;
        ui64 FirstLsn = ui64(-1);
        ui64 LastLsn = 0;
        // Records totally inserted (can be more than stored because of compaction)
        ui64 Inserts = 0;

        void UpdateMetadataOnAdd(const TAppendix &a, ui64 firstLsn, ui64 lastLsn) {
            Y_DEBUG_ABORT_UNLESS(firstLsn != ui64(-1) && firstLsn <= lastLsn && LastLsn < lastLsn && !a.Empty());
            Inserts += a.GetSize();
            if (FirstLsn == ui64(-1)) {
                FirstLsn = firstLsn;
            }
            LastLsn = lastLsn;
        }

    public:
        TFreshAppendixTree(const THullCtxPtr &hullCtx, size_t stagingCapacity)
            : Tree(hullCtx, stagingCapacity)
        {}

        void AddAppendix(const TAppendixPtr &a, ui64 firstLsn, ui64 lastLsn) {
            UpdateMetadataOnAdd(*a, firstLsn, lastLsn);
            Tree.Add(a);
        }

        void AddAppendix(TAppendixPtr &&a, ui64 firstLsn, ui64 lastLsn) {
            UpdateMetadataOnAdd(*a, firstLsn, lastLsn);
            Tree.Add(std::move(a));
        }

        bool Empty() const { return FirstLsn == ui64(-1); }
        ui64 GetFirstLsn() const { return FirstLsn; }
        ui64 GetLastLsn() const { return LastLsn; }
        ui64 ElementsInserted() const { return Inserts; }
        ui64 SizeApproximation() const { return Tree.SizeApproximation(); }

        TFreshAppendixTreeSnap<TKey, TMemRec> GetSnapshot() const {
            return TFreshAppendixTreeSnap<TKey, TMemRec>(Tree.GetSnapshot());
        }

        std::shared_ptr<ISTreeCompaction> Compact() {
            return Tree.Compact();
        }

        std::shared_ptr<ISTreeCompaction> ApplyCompactionResult(std::shared_ptr<ISTreeCompaction> cjob) {
            return Tree.ApplyCompactionResult(cjob);
        }

        const TSTreeCompactionStat &GetCompactionStat() const {
            return Tree.GetCompactionStat();
        }

        void OutputHtml(IOutputStream &str) const {
            auto snap = GetSnapshot();
            typename TFreshAppendixTreeSnap<TKey, TMemRec>::TAppendixIterator it(&snap);
            it.SeekToFirst();
            ui64 number = 0;

            HTML(str) {
                TABLE_CLASS ("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "AppendixNumber";}
                            TABLEH() {str << "Items";}
                            TABLEH() {str << "Bytes";}
                        }
                    }
                    TABLEBODY() {
                        while (it.Valid()) {
                            TABLER() {
                                TABLED() { str << number; }
                                TABLED() { str << it.Get()->GetSize(); }
                                TABLED() { str << it.Get()->SizeApproximation(); }
                            }
                            it.Next();
                            ++number;
                        }
                    }
                }
            }
        }
    };

    extern template class TFreshAppendixTree<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshAppendixTree<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshAppendixTree<TKeyBlock, TMemRecBlock>;

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendixTreeSnap::TPrivateIteratorBase
    // Base class for TFreshAppendixTreeSnap iterators
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendixTreeSnap<TKey, TMemRec>::TPrivateIteratorBase {
    protected:
        using TSnap = ::NKikimr::TFreshAppendixTreeSnap<TKey, TMemRec>;
        using TFreshAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;
        using TAppendixIterator = typename TFreshAppendix::TIterator;

        const TSnap *Snap;

        TPrivateIteratorBase(const TSnap *snap)
            : Snap(snap)
        {}
        TPrivateIteratorBase(const TPrivateIteratorBase &) = default;
        TPrivateIteratorBase &operator=(const TPrivateIteratorBase &) = default;

        static TVector<TFreshAppendix*> Convert(const TSnap *snap) {
            if (!snap)
                return {};

            TVector<TFreshAppendix*> result;
            typename TSnap::TTreeSnap::TIterator it(&snap->Snap);
            it.SeekToFirst();
            while (it.Valid()) {
                result.push_back(it.Get().get());
                it.Next();
            }
            return result;
        }

    public:
        void DumpAll(IOutputStream &str) const {
            str << "TFreshAppendixTreeSnap";
        }
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendixTreeSnap::TForwardIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendixTreeSnap<TKey, TMemRec>::TForwardIterator :
        public TPrivateIteratorBase,
        public TGenericNWayForwardIterator<TKey, typename ::NKikimr::TFreshAppendix<TKey, TMemRec>::TIterator> {
    public:
        using TSnap = ::NKikimr::TFreshAppendixTreeSnap<TKey, TMemRec>;
        using TBase = TGenericNWayForwardIterator<TKey, typename TPrivateIteratorBase::TAppendixIterator>;
        using TContType = TSnap;

        TForwardIterator(const THullCtxPtr &hullCtx, const TSnap *snap)
            : TPrivateIteratorBase(snap)
            , TBase(hullCtx, this->Convert(TPrivateIteratorBase::Snap))
        {}

        TForwardIterator(TForwardIterator &&) = default;
        TForwardIterator& operator=(TForwardIterator &&) = default;
        TForwardIterator(const TForwardIterator &) = default;
        TForwardIterator &operator=(const TForwardIterator &) = default;

        using TBase::Valid;
        using TBase::Next;
        using TBase::GetCurKey;
        using TBase::Seek;
        using TBase::SeekToFirst;
        using TBase::PutToMerger;
        using TBase::PutToHeap;
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshAppendixTreeSnap::TBackwardIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendixTreeSnap<TKey, TMemRec>::TBackwardIterator :
        public TPrivateIteratorBase,
        public TGenericNWayBackwardIterator<TKey, typename ::NKikimr::TFreshAppendix<TKey, TMemRec>::TIterator> {
    public:
        using TSnap = ::NKikimr::TFreshAppendixTreeSnap<TKey, TMemRec>;
        using TBase = TGenericNWayBackwardIterator<TKey, typename TPrivateIteratorBase::TAppendixIterator>;
        using TContType = TSnap;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TSnap *snap)
            : TPrivateIteratorBase(snap)
            , TBase(hullCtx, this->Convert(TPrivateIteratorBase::Snap))
        {}

        TBackwardIterator(TBackwardIterator &&) = default;
        TBackwardIterator& operator=(TBackwardIterator &&) = default;
        TBackwardIterator(const TBackwardIterator &) = default;
        TBackwardIterator &operator=(const TBackwardIterator &) = default;

        using TBase::Valid;
        using TBase::Prev;
        using TBase::GetCurKey;
        using TBase::Seek;
        using TBase::ToString;
        using TBase::PutToMerger;
        using TBase::PutToHeap;
    };

} // NKikimr

