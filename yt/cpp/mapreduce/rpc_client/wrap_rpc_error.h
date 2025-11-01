#pragma once

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/threading/future/core/future.h>


namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TErrorResponse ToErrorResponse(TErrorException ex);

////////////////////////////////////////////////////////////////////////////////

template <typename TResult>
::NThreading::TFuture<TResult> WrapRpcError(::NThreading::TFuture<TResult> future) {
    return future.Apply(([](auto&& tryResult){
        try {
            tryResult.TryRethrow();
            return std::forward<decltype(tryResult)>(tryResult);
        } catch (TErrorException ex) {
            return ::NThreading::MakeErrorFuture<TResult>(std::make_exception_ptr(ToErrorResponse(std::move(ex))));
        }
    }));
}

template <typename TResult>
TResult WaitAndProcess(TFuture<TResult> future) {
    try {
        if constexpr (std::is_same_v<TResult, void>) {
            NConcurrency::WaitFor(future).ThrowOnError();
        } else {
            auto result = NConcurrency::WaitFor(future).ValueOrThrow();
            return result;
        }
    } catch (TErrorException ex) {
        throw ToErrorResponse(std::move(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
