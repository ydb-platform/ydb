#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <library/cpp/threading/future/future.h>

#include <memory>
#include <type_traits>
#include <utility>

namespace NKikimr::NKqp {

// Complete an owned span on the future's callback thread before releasing its
// consumer. end(span, result) receives a pointer to the value, or null for an
// exception (a success bool for a void future). Captures no compiler state.
template <class T, class TEnd>
NThreading::TFuture<T> TraceFuture(NThreading::TFuture<T> future, NWilson::TSpan span, TEnd end) {
    if (!span) {
        return future;
    }
    return future.Apply([span = std::make_shared<NWilson::TSpan>(std::move(span)), end = std::move(end)]
        (const NThreading::TFuture<T>& ready) mutable -> T {
        try {
            ready.TryRethrow();
        } catch (...) {
            if constexpr (std::is_void_v<T>) {
                end(*span, false);
            } else {
                end(*span, static_cast<const T*>(nullptr));
            }
            throw;
        }
        if constexpr (std::is_void_v<T>) {
            end(*span, true);
        } else {
            const T& result = ready.GetValue();
            end(*span, &result);
            return result;
        }
    });
}

} // namespace NKikimr::NKqp
