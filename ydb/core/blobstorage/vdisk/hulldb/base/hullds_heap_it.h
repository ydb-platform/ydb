#pragma once
#include "defs.h"
#include <algorithm>
#include "hullds_ut.h"
#include "hullds_generic_it.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstslice_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_segment_impl.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_appendix.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstvec_it.h>

template<typename T>
concept HasFirst = T::First();

namespace NKikimr {

    template <typename TKey, typename TMemRec, bool Forward> 
    class THeapIterator {
    protected:
        struct THeapItem {
            //using TLevelIndexSnapshotForwardIt = TLevelIndexSnapshot<TKey, TMemRec>::TForwardIterator*;
            using TFreshIndexAndDataIterator = std::conditional_t<Forward, typename TFreshIndexAndDataSnapshot<TKey, TMemRec>::TForwardIterator, 
                                                                  typename TFreshIndexAndDataSnapshot<TKey, TMemRec>::TBackwardIterator>;
            using TIterators = std::variant<TVectorIt*, TFreshIndexAndDataIterator*, typename TFreshAppendix<TKey, TMemRec>::TIterator*,
                                            typename TOrderedLevelSegments<TKey, TMemRec>::TReadIterator*,
                                            typename TLevelSegment<TKey, TMemRec>::TMemIterator*>;
            TKey Key;
            TIterators Iter;

            friend bool operator<(const THeapItem& x, const THeapItem& y) { 
                if constexpr (Forward) {
                    return y.Key < x.Key;
                }
                else{
                    return x.Key < y.Key;
                }
            } 

            template <class TIter>
            THeapItem(TIter *iter) {
                Iter = iter;
            }
        };

        std::vector<THeapItem> Heap;
        size_t HeapItems = 0;

        template<typename TMerger>
        void Move(TMerger merger) {
            Y_ABORT_UNLESS(Valid());
            const TKey key = Heap.front().Key;
            while (HeapItems && Heap.front().Key == key) {
                std::pop_heap(Heap.begin(), Heap.begin() + HeapItems);
                THeapItem& item = Heap[HeapItems - 1];
                const bool remainsValid = std::visit([&](auto *iter) {
                    if constexpr (!std::is_same_v<TMerger, std::nullptr_t>) {
                        iter->PutToMerger(merger);
                    }
                    if constexpr (Forward) {
                        iter->Next();
                    }
                    else{
                        iter->Prev();
                    }
                    if (iter->Valid()) {
                        item.Key = iter->GetCurKey();
                        return true;
                    } else {
                        return false;
                    }
                }, item.Iter);
                if (remainsValid) {
                    std::push_heap(Heap.begin(), Heap.begin() + HeapItems);
                } else {
                    --HeapItems;
                }
            }
        }

    public:
        bool Valid() const {
            return HeapItems;
        }

        template <class TIter> 
        void Add(TIter* iter) {
            THeapItem item(iter);
            Heap.push_back(item);
            HeapItems++;
        }

        template<typename TMerger>
        void PutToMergerAndAdvance(TMerger *merger) {
            Move(merger);
        } 

        void Next() {
            Move(nullptr);
        }

        void Prev() {
            Move(nullptr);
        }

        void Seek(const TKey &key) {
            auto pivot = [&](THeapItem& item) {
                return std::visit([&] (auto *iter) {
                    iter->Seek(key);
                    if constexpr (Forward) {
                        if (iter->Valid()) {
                            item.Key = iter->GetCurKey();
                            return true;
                        }
                        return false;
                    }
                    else{
                        if (!iter->Valid()) {
                            iter->Prev();
                            item.Key = iter->GetCurKey();
                            return true;
                        }
                        else{
                            if (iter->GetCurKey() == key) {
                                item.Key = iter->GetCurKey();
                                return true;
                            }
                            else{
                                iter->Prev();
                                if (iter->Valid()) {
                                    item.Key = iter->GetCurKey();
                                    return true;
                                }
                                return false;
                            }
                        }
                    }
                }, item.Iter); 
            }; 
            auto it = std::partition(Heap.begin(), Heap.end(), pivot); 
            std::make_heap(Heap.begin(), it);
            HeapItems = it - Heap.begin();
        }

        void SeekToFirst() {
            if constexpr (HasFirst<TKey>) {
                Seek(TKey::First());
            }
            else{
                Seek(TKey{});
            }
        }

        template<typename TMerger>
        void Walk(TKey key, TMerger merger, std::function<bool(const TKey&, TMerger)> callback) {
            for (int idx = 0; idx < HeapItems; idx++) {
                auto *iter = Heap[idx];
                iter->Seek(key);
                while (iter->Valid()) {
                    iter->PutToMerger(&merger);
                    if (!callback(iter->GetCurKey(), merger)) {
                        break;
                    }
                    merger.Clear();
                    if constexpr (Forward) {
                        iter->Next();
                    }
                    else{
                        iter->Prev();
                    }
                }
            }
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Heap.front().Key;
        }
    };
    
} // NKikimr