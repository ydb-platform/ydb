#pragma once

#include "byte_size.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/datetime/base.h>

namespace NSQLComplete {

    namespace NPrivate {

        template <class T>
        concept CHashable = requires(const T& x) {
            { THash<T>()(x) } -> std::convertible_to<std::size_t>;
        };

        template <class T>
        concept CCacheKey = std::regular<T> && CHashable<T> && CByteSized<T>;

        template <class T>
        concept CCacheValue = std::copyable<T> && CByteSized<T>;

    }; // namespace NPrivate

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class ICache: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<ICache>;

        struct TEntry {
            TMaybe<TValue> Value = Nothing();
            bool IsExpired = true;
        };

        virtual ~ICache() = default;
        virtual NThreading::TFuture<TEntry> Get(const TKey& key) const = 0;
        virtual NThreading::TFuture<void> Update(const TKey& key, TValue value) const = 0;
    };

} // namespace NSQLComplete
