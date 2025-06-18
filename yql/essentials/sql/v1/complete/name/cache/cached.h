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
            return Cache_->Get(key).Apply([cache = Cache_,
                                           query = Query_,
                                           key = std::move(key)](auto f) {
                typename ICache<TKey, TValue>::TEntry entry = f.ExtractValue();
                if (entry.IsExpired) {
                    query(key).Apply([cache, key = std::move(key)](auto f) {
                        cache->Update(key, f.ExtractValue());
                    });
                }
                return std::move(entry.Value);
            });
        }

    private:
        ICache<TKey, TValue>::TPtr Cache_;
        TFunc Query_;
    };

} // namespace NSQLComplete
