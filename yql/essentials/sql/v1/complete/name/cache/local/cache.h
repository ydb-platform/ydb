#pragma once

#include <yql/essentials/sql/v1/complete/name/cache/cache.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/time_provider/monotonic_provider.h>

#include <util/system/mutex.h>

namespace NSQLComplete {

    struct TLocalCacheConfig {
        size_t ByteCapacity = 1 * 1024 * 1024;
        TDuration TTL = TDuration::Seconds(8);
    };

    namespace NPrivate {

        template <CCacheValue TValue>
        struct TLocalCacheCell {
            TValue Value;
            NMonotonic::TMonotonic Deadline;
            size_t KeyByteSize = 0;
            size_t CellByteSize = 0;
        };

        template <CCacheKey TKey, CCacheValue TValue>
        class TLocalCache: public ICache<TKey, TValue> {
        private:
            using TEntry = ICache<TKey, TValue>::TEntry;
            using TCell = TLocalCacheCell<TValue>;

            struct TLRUSizeProvider {
                size_t operator()(const TCell& x) noexcept {
                    const size_t listItemContent = x.CellByteSize;
                    const size_t listItemPtrs = sizeof(TIntrusiveListItem<void>);
                    const size_t listItem = listItemContent + listItemPtrs;

                    const size_t cacheIndexKey = x.KeyByteSize;
                    const size_t cacheIndexListItemPtr = sizeof(void*);
                    const size_t cacheIndexEntry = cacheIndexKey + cacheIndexListItemPtr;

                    return listItem + cacheIndexEntry;
                }
            };

            using TStorage = TLRUCache<TKey, TCell, TNoopDelete, TLRUSizeProvider>;

        public:
            TLocalCache(TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> clock, TLocalCacheConfig config)
                : Clock_(std::move(clock))
                , Config_(std::move(config))
                , Origin_(/* maxSize = */ Config_.ByteCapacity)
            {
            }

            NThreading::TFuture<TEntry> Get(const TKey& key) const override {
                TEntry entry;

                with_lock (Mutex_) {
                    if (auto it = Origin_.Find(key); it != Origin_.End()) {
                        entry.Value = it->Value;
                        entry.IsExpired = (it->Deadline < Clock_->Now());
                    }
                }

                return NThreading::MakeFuture(std::move(entry));
            }

            NThreading::TFuture<void> Update(const TKey& key, TValue value) const override {
                TCell cell = {
                    .Value = std::move(value),
                    .Deadline = Clock_->Now() + Config_.TTL,
                    .KeyByteSize = TByteSize<TKey>()(key),
                };

                cell.CellByteSize =
                    TByteSize<TValue>()(cell.Value) +
                    sizeof(cell.Deadline) +
                    cell.KeyByteSize;

                with_lock (Mutex_) {
                    Origin_.Update(key, std::move(cell));
                }

                return NThreading::MakeFuture();
            }

        private:
            TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> Clock_;
            TLocalCacheConfig Config_;

            TMutex Mutex_;
            mutable TStorage Origin_;
        };

    } // namespace NPrivate

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    ICache<TKey, TValue>::TPtr MakeLocalCache(
        TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> clock,
        TLocalCacheConfig config) {
        return new NPrivate::TLocalCache<TKey, TValue>(std::move(clock), std::move(config));
    }

    template <NPrivate::CCacheValue TValue>
    struct TByteSize<NPrivate::TLocalCacheCell<TValue>> {
        size_t operator()(const NPrivate::TLocalCacheCell<TValue>& x) const noexcept {
            return x.CellByteSize;
        }
    };

} // namespace NSQLComplete
