#ifndef ARCADIA_INTEROP_INL_H_
#error "Direct inclusion of this file is not allowed, include async_batcher.h"
// For the sake of sane code completion.
#include "arcadia_interop.h"
#endif
#undef ARCADIA_INTEROP_INL_H_

#include <yt/yt/core/actions/future.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
::NThreading::TFuture<T> ToArcadiaFuture(const TFuture<T>& future)
{
    auto promise = ::NThreading::NewPromise<T>();
    auto wrappedFuture = promise.GetFuture();

    future
        .Subscribe(BIND([promise = std::move(promise)] (const TErrorOr<T>& valueOrError) mutable {
            try {
                if constexpr (std::is_same_v<T, void>) {
                    valueOrError
                        .ThrowOnError();
                    promise.TrySetValue();
                } else {
                    auto value = valueOrError
                        .ValueOrThrow();
                    promise.TrySetValue(std::move(value));
                }
            } catch (...) {
                promise.TrySetException(std::current_exception());
            }
        }));

    return wrappedFuture;
}

template <class T>
TFuture<T> FromArcadiaFuture(const ::NThreading::TFuture<T>& future)
{
    auto promise = NewPromise<T>();
    auto wrappedFuture = promise.ToFuture();

    future
        .Subscribe([promise = std::move(promise)](::NThreading::TFuture<T> future) {
            YT_ASSERT(future.HasValue() || future.HasException());
            try {
                if constexpr (std::is_void_v<T>) {
                    future.TryRethrow();
                    promise.TrySet();
                } else {
                    promise.TrySet(future.ExtractValueSync());
                }
            } catch (const std::exception& e) {
                promise.TrySet(NYT::TError(e));
            }
        });

    return wrappedFuture;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
