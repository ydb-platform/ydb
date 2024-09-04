#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>
#include <util/generic/queue.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TGenericForwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TIter1, class TIter2>
    class TGenericForwardIterator {
    public:
        typedef typename TIter1::TContType TType1;
        typedef typename TIter2::TContType TType2;

        TGenericForwardIterator(const THullCtxPtr &hullCtx, const TType1 *cont1, const TType2 *cont2)
            : Iter1(hullCtx, cont1)
            , Iter2(hullCtx, cont2)
        {}

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            if (!Iter1.Valid()) {
                Iter2.PutToMerger(merger);
            } else if (!Iter2.Valid()) {
                Iter1.PutToMerger(merger);
            } else if (Iter1.GetCurKey() < Iter2.GetCurKey()) {
                Iter1.PutToMerger(merger);
            } else if (Iter2.GetCurKey() < Iter1.GetCurKey()) {
                Iter2.PutToMerger(merger);
            } else {
                Iter1.PutToMerger(merger);
                Iter2.PutToMerger(merger);
            }
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            Iter1.PutToHeap(heap);
            Iter2.PutToHeap(heap);
        }

        bool Valid() const {
            return Iter1.Valid() || Iter2.Valid();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());

            if (!Iter1.Valid()) {
                Iter2.Next();
            } else if (!Iter2.Valid()) {
                Iter1.Next();
            } else if (Iter1.GetCurKey() < Iter2.GetCurKey()) {
                Iter1.Next();
            } else if (Iter2.GetCurKey() < Iter1.GetCurKey()) {
                Iter2.Next();
            } else {
                Iter1.Next();
                Iter2.Next();
            }
        }

        // TODO: cache keys?
        TKey GetCurKey() const {
            if (!Iter1.Valid()) {
                return Iter2.GetCurKey();
            } else if (!Iter2.Valid()) {
                return Iter1.GetCurKey();
            } else {
                TKey key1 = Iter1.GetCurKey();
                TKey key2 = Iter2.GetCurKey();
                return (key1 < key2) ? key1 : key2;
            }
        }

        void Seek(const TKey &key) {
            Iter1.Seek(key);
            Iter2.Seek(key);
        }

        void SeekToFirst() {
            Iter1.SeekToFirst();
            Iter2.SeekToFirst();
        }

        void DumpAll(IOutputStream &str) const {
            Iter1.DumpAll(str);
            Iter2.DumpAll(str);
        }

    protected:
        TIter1 Iter1;
        TIter2 Iter2;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TGenericBackwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TIter1, class TIter2>
    class TGenericBackwardIterator {
    public:
        typedef typename TIter1::TContType TType1;
        typedef typename TIter2::TContType TType2;

        TGenericBackwardIterator(const THullCtxPtr &hullCtx, const TType1 *cont1, const TType2 *cont2)
            : Iter1(hullCtx, cont1)
            , Iter2(hullCtx, cont2)
        {}

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            if (!Iter1.Valid()) {
                Iter2.PutToMerger(merger);
            } else if (!Iter2.Valid()) {
                Iter1.PutToMerger(merger);
            } else if (Iter1.GetCurKey() < Iter2.GetCurKey()) {
                Iter2.PutToMerger(merger);
            } else if (Iter2.GetCurKey() < Iter1.GetCurKey()) {
                Iter1.PutToMerger(merger);
            } else {
                Iter1.PutToMerger(merger);
                Iter2.PutToMerger(merger);
            }
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            Iter1.PutToHeap(heap);
            Iter2.PutToHeap(heap);
        }

        bool Valid() const {
            return Iter1.Valid() || Iter2.Valid();
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Valid());

            if (!Iter1.Valid()) {
                Iter2.Prev();
            } else if (!Iter2.Valid()) {
                Iter1.Prev();
            } else if (Iter1.GetCurKey() < Iter2.GetCurKey()) {
                Iter2.Prev();
            } else if (Iter2.GetCurKey() < Iter1.GetCurKey()) {
                Iter1.Prev();
            } else {
                Iter1.Prev();
                Iter2.Prev();
            }
        }

        // TODO: cache keys?
        TKey GetCurKey() const {
            if (!Iter1.Valid()) {
                return Iter2.GetCurKey();
            } else if (!Iter2.Valid()) {
                return Iter1.GetCurKey();
            } else {
                TKey key1 = Iter1.GetCurKey();
                TKey key2 = Iter2.GetCurKey();
                return (key1 < key2) ? key2 : key1;
            }
        }

        void Seek(const TKey &key) {
            Iter1.Seek(key);
            Iter2.Seek(key);
        }

    protected:
        TIter1 Iter1;
        TIter2 Iter2;
    };


    ////////////////////////////////////////////////////////////////////////////
    // NHullPrivate
    ////////////////////////////////////////////////////////////////////////////
    namespace NHullPrivate {

        template <class TKey, class TIter, class TRecordMerger, class TPQueue>
        void PutToMerger(TRecordMerger *merger, TPQueue &pqueue) {
            using TIterPtr = std::shared_ptr<TIter>;
            Y_DEBUG_ABORT_UNLESS(!pqueue.empty());
            TStackVec<TIterPtr, 32> tmp;
            TIterPtr it = pqueue.top();
            pqueue.pop();
            tmp.push_back(it);
            TKey curKey = it->GetCurKey();
            it->PutToMerger(merger);
            while (!pqueue.empty() && curKey == (it = pqueue.top())->GetCurKey()) {
                pqueue.pop();
                tmp.push_back(it);
                it->PutToMerger(merger);
            }

            // return iterators back
            for (const auto x : tmp)
                pqueue.push(x);
        }

        template <class TKey, class TIter, class TPQueue>
        TString ToString(TPQueue &pqueue) {
            using TIterPtr = std::shared_ptr<TIter>;
            TStringStream str;
            Y_DEBUG_ABORT_UNLESS(!pqueue.empty());
            TStackVec<TIterPtr, 32> tmp;
            TIterPtr it = pqueue.top();
            Y_DEBUG_ABORT_UNLESS(it->Valid());
            pqueue.pop();
            tmp.push_back(it);
            str << it->ToString();
            TKey curKey = it->GetCurKey();
            while (!pqueue.empty() && curKey == (it = pqueue.top())->GetCurKey()) {
                pqueue.pop();
                tmp.push_back(it);
                str << " " << it->ToString();
            }

            // return iterators back
            for (const auto x : tmp)
                pqueue.push(x);

            return str.Str();
        }

        template <class TPQueue, class TIter>
        void Copy(
                TVector<std::shared_ptr<TIter>> &dstIters,
                TPQueue &dstPQueue,
                const TVector<std::shared_ptr<TIter>> &srcIters)
        {
            // copy iterators
            dstIters.clear();
            dstPQueue.clear();
            dstIters.reserve(srcIters.size());
            for (const auto &x : srcIters) {
                dstIters.push_back(std::make_shared<TIter>(*x));
            }
            // set up PQueue
            for (auto &x : dstIters) {
                if (x->Valid()) {
                    dstPQueue.push(x);
                }
            }
        }

    } // NHullPrivate

    ////////////////////////////////////////////////////////////////////////////
    // TGenericNWayForwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TIter>
    class TGenericNWayForwardIterator {
    public:
        using TIterContType = typename TIter::TContType;

        TGenericNWayForwardIterator(TGenericNWayForwardIterator &&) = default;
        TGenericNWayForwardIterator &operator=(TGenericNWayForwardIterator &&) = default;
        TGenericNWayForwardIterator(const TGenericNWayForwardIterator &c) {
            NHullPrivate::Copy<TPQueue, TIter>(Iters, PQueue, c.Iters);
        }
        TGenericNWayForwardIterator &operator=(const TGenericNWayForwardIterator &c) {
            NHullPrivate::Copy<TPQueue, TIter>(Iters, PQueue, c.Iters);
        }
        TGenericNWayForwardIterator(const THullCtxPtr &hullCtx, const TVector<TIterContType*> &elements) {
            Iters.reserve(elements.size());
            for (const auto &x : elements) {
                if (!x->Empty()) {
                    Iters.push_back(std::make_shared<TIter>(hullCtx, x));
                }
            }
        }

        bool Valid() const {
            return !PQueue.empty();
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return PQueue.top()->GetCurKey();
        }

        void SeekToFirst() {
            TPQueue().swap(PQueue); // clear PQueue
            for (auto &x : Iters) {
                x->SeekToFirst();
                if (x->Valid()) {
                    PQueue.push(x);
                }
            }
        }

        void Seek(const TKey &key) {
            TPQueue().swap(PQueue); // clear PQueue
            for (auto &x : Iters) {
                x->Seek(key);
                if (x->Valid()) {
                    PQueue.push(x);
                }
            }
        }

        void Next() {
            TIterPtr it = PQueue.top();
            PQueue.pop();
            Y_DEBUG_ABORT_UNLESS(it->Valid());
            TKey curKey = it->GetCurKey();
            it->Next();
            if (it->Valid()) {
                PQueue.push(it);
            }

            while (!PQueue.empty() && curKey == ((it = PQueue.top())->GetCurKey())) {
                PQueue.pop();
                it->Next();
                if (it->Valid()) {
                    PQueue.push(it);
                }
            }
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            NHullPrivate::PutToMerger<TKey, TIter, TRecordMerger, TPQueue>(merger, PQueue);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            for (auto &x : Iters) {
                x->PutToHeap(heap);
            }
        }

        TString ToString() const {
            return NHullPrivate::ToString<TKey, TIter, TPQueue>(PQueue);
        }

    protected:
        using TIterPtr = std::shared_ptr<TIter>;
        class TGreater {
        public:
            bool operator () (const TIterPtr &c1, const TIterPtr &c2) const {
                return c2->GetCurKey() < c1->GetCurKey();
            }
        };
        using TPQueue = TPriorityQueue<TIterPtr, TVector<TIterPtr>, TGreater>;

        TVector<TIterPtr> Iters;
        mutable TPQueue PQueue;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TGenericNWayBackwardIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TIter>
    class TGenericNWayBackwardIterator {
    public:
        using TIterContType = typename TIter::TContType;

        TGenericNWayBackwardIterator(TGenericNWayBackwardIterator &&) = default;
        TGenericNWayBackwardIterator &operator=(TGenericNWayBackwardIterator &&) = default;
        TGenericNWayBackwardIterator(const TGenericNWayBackwardIterator &c) {
            NHullPrivate::Copy<TPQueue, TIter>(Iters, PQueue, c.Iters);
        }
        TGenericNWayBackwardIterator &operator=(const TGenericNWayBackwardIterator &c) {
            NHullPrivate::Copy<TPQueue, TIter>(Iters, PQueue, c.Iters);
        }
        TGenericNWayBackwardIterator(const THullCtxPtr &hullCtx, const TVector<TIterContType*> &elements) {
            Iters.reserve(elements.size());
            for (const auto &x : elements) {
                if (!x->Empty()) {
                    Iters.push_back(std::make_shared<TIter>(hullCtx, x));
                }
            }
        }

        bool Valid() const {
            return !PQueue.empty();
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return PQueue.top()->GetCurKey();
        }

        void Seek(const TKey &key) {
            TPQueue().swap(PQueue); // clear PQueue
            for (auto &x : Iters) {
                x->Seek(key);

                if (!x->Valid()) {
                    x->Prev();
                    PQueue.push(x);
                } else {
                    if (x->GetCurKey() == key) {
                        PQueue.push(x);
                    } else {
                        x->Prev();
                        if (x->Valid()) {
                            PQueue.push(x);
                        }
                    }
                }
            }
        }

        void Prev() {
            TIterPtr it = PQueue.top();
            PQueue.pop();
            Y_DEBUG_ABORT_UNLESS(it->Valid());
            TKey curKey = it->GetCurKey();
            it->Prev();
            if (it->Valid()) {
                PQueue.push(it);
            }

            while (!PQueue.empty() && curKey == ((it = PQueue.top())->GetCurKey())) {
                PQueue.pop();
                it->Prev();
                if (it->Valid()) {
                    PQueue.push(it);
                }
            }
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            NHullPrivate::PutToMerger<TKey, TIter, TRecordMerger, TPQueue>(merger, PQueue);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            for (auto &x : Iters) {
                x->PutToHeap(heap);
            }
        }

        TString ToString() const {
            return NHullPrivate::ToString<TKey, TIter, TPQueue>(PQueue);
        }

    protected:
        using TIterPtr = std::shared_ptr<TIter>;
        class TLess {
        public:
            bool operator () (const TIterPtr &c1, const TIterPtr &c2) const {
                if (!c1->Valid())
                    return false;
                if (!c2->Valid())
                    return true;
                return c1->GetCurKey() < c2->GetCurKey();
            }
        };
        using TPQueue = TPriorityQueue<TIterPtr, TVector<TIterPtr>, TLess>;

        TVector<TIterPtr> Iters;
        mutable TPQueue PQueue;
    };

} // NKikimr

