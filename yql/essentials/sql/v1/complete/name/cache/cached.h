#pragma once

#include "cache.h"

namespace NSQLComplete {

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TCachedQuery {
    public:
        using TFunc = std::function<NThreading::TFuture<TValue>(const TKey& key)>;

        TCachedQuery(ICache<TKey, TValue>::TPtr cache, TFunc query)
            : Cache_(std::move(cache))
            , Query_(std::move(query))
        {
        }

        NThreading::TFuture<TValue> operator()(TKey key) const {
            auto promise = NThreading::NewPromise<TValue>();
            Cache_->Get(key).Apply([cache = Cache_,
                                    query = Query_,
                                    key = std::move(key),
                                    promise](auto f) mutable {
                typename ICache<TKey, TValue>::TEntry entry;
                try {
                    entry = f.ExtractValue();
                } catch (...) {
                    promise.SetException(std::current_exception());
                    return;
                }

                if (!entry.IsExpired) {
                    Y_ENSURE(entry.Value.Defined());
                    promise.SetValue(std::move(*entry.Value));
                    return;
                }

                bool isEmpty = entry.Value.Empty();
                if (!isEmpty) {
                    promise.SetValue(std::move(*entry.Value));
                }

                query(key).Apply([cache, key = std::move(key), isEmpty, promise](auto f) mutable {
                    TValue value;
                    try {
                        value = f.ExtractValue();
                    } catch (...) {
                        promise.SetException(std::current_exception());
                        return;
                    }

                    if (isEmpty) {
                        promise.SetValue(value);
                    }

                    cache->Update(key, std::move(value));
                });
            });
            return promise;
        }

    private:
        ICache<TKey, TValue>::TPtr Cache_;
        TFunc Query_;
    };

} // namespace NSQLComplete
