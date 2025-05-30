#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_segment.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_appendix.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstvec_it.h>
#include <algorithm>

namespace NKikimr {

    template <typename TKey, typename TMemRec, bool Forward>
    class THeapIterator {
    protected:
        struct THeapItem {
            using TFreshIndexAndDataIterator = std::conditional_t<Forward, typename TFreshIndexAndDataSnapshot<TKey, TMemRec>::TForwardIterator,
                                                                  typename TFreshIndexAndDataSnapshot<TKey, TMemRec>::TBackwardIterator>;
            using TIterators = std::variant<TFreshIndexAndDataIterator*, typename TFreshAppendix<TKey, TMemRec>::TIterator*,
                                            typename TOrderedLevelSegments<TKey, TMemRec>::TReadIterator*,
                                            typename TLevelSegment<TKey, TMemRec>::TMemIterator*>;
            TKey Key;
            TIterators Iter;

            friend bool operator<(const THeapItem& x, const THeapItem& y) {
                if constexpr (Forward) {
                    return y.Key < x.Key;
                } else {
                    return x.Key < y.Key;
                }
            }

            template <class TIter>
            THeapItem(TIter *iter) : Iter(iter) {}
        };

        std::vector<THeapItem> Heap;
        size_t HeapItems = 0;
        bool Initialized = false;

        template<typename TMerger>
        void Move(TMerger merger) {
            Y_ABORT_UNLESS(Valid() && Initialized);
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
                    } else {
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
        template <typename TIter>
        THeapIterator(TIter* iter) {
            iter->PutToHeap(*this);
        }

        THeapIterator() = default;

        bool Valid() const {
            return HeapItems;
        }

        template <class TIter>
        void Add(TIter* iter) {
            THeapItem item(iter);
            Heap.push_back(item);
        }

        template<typename TMerger>
        void PutToMergerAndAdvance(TMerger *merger) {
            Move(merger);
        }

        void Next() {
            static_assert(Forward);
            Move(nullptr);
        }

        void Prev() {
            static_assert(!Forward);
            Move(nullptr);
        }

        template<typename T>
        void SeekImpl(T&& callback) {
            auto pivot = [&](THeapItem& item) {
                return std::visit([&](auto *iter) {
                    callback(iter);
                    const bool valid = iter->Valid();
                    if (valid) {
                        item.Key = iter->GetCurKey();
                    }
                    return valid;
                }, item.Iter);
            };
            auto it = std::partition(Heap.begin(), Heap.end(), pivot);
            std::make_heap(Heap.begin(), it);
            HeapItems = it - Heap.begin();
            Initialized = true;
        };

        void Seek(const TKey &key) {
            return SeekImpl([&](auto *iter) {
                iter->Seek(key);
                if constexpr (!Forward && !std::is_same_v<std::decay_t<decltype(*iter)>,
                        typename NKikimr::TFreshIndexAndDataSnapshot<TKey, TMemRec>::TBackwardIterator>) {
                    if (!iter->Valid() || key < iter->GetCurKey()) {
                        iter->Prev();
                    }
                }
                Y_DEBUG_ABORT_UNLESS(!iter->Valid() || (Forward ? key <= iter->GetCurKey() : iter->GetCurKey() <= key));
            });
        }

        void SeekToFirst() {
            return SeekImpl([](auto *iter) { iter->SeekToFirst(); });
        }

        template<typename TMerger, typename TCallback>
        void Walk(std::optional<TKey> key, TMerger merger, TCallback&& callback) {
            if (key.has_value()) {
                Seek(key.value());
            } else {
                SeekImpl([](auto*) {}); // just continue where it took off
            }
            while (Valid()) {
                const TKey key = GetCurKey();
                PutToMergerAndAdvance(merger);
                merger->Finish();
                const bool res = callback(key, merger);
                merger->Clear();
                if (!res) {
                    break;
                }
            }
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Heap.front().Key;
        }
    };

} // NKikimr
