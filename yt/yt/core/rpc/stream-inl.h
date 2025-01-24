#ifndef STREAM_INL_H_
#error "Direct inclusion of this file is not allowed, include stream.h"
// For the sake of sane code completion.
#include "stream.h"
#endif

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateRpcClientInputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request)
{
    auto invokeResult = request->Invoke().template As<void>();
    return request->GetRequestAttachmentsStream()->Close().Apply(BIND([=] () {
        return New<NDetail::TRpcClientInputStream>(
            std::move(request),
            std::move(invokeResult));
    })).template As<NConcurrency::IAsyncZeroCopyInputStreamPtr>();
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    bool feedbackEnabled)
{
    auto invokeResult = request->Invoke().template As<void>();
    return NDetail::CreateRpcClientOutputStreamFromInvokedRequest(
        std::move(request),
        std::move(invokeResult),
        feedbackEnabled);
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    TCallback<void(TSharedRef)> metaHandler)
{
    auto invokeResult = request->Invoke().template As<void>();
    auto metaHandlerResult = request->GetResponseAttachmentsStream()->Read()
        .Apply(std::move(metaHandler));
    return metaHandlerResult.Apply(BIND ([req = std::move(request), res = std::move(invokeResult)] () mutable {
        return NDetail::CreateRpcClientOutputStreamFromInvokedRequest(
            std::move(req),
            std::move(res));
    }));
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    TCallback<void(TSharedRef)> metaHandler,
    TCallback<void(TIntrusivePtr<TResponse>&&)> rspHandler)
{
    auto invokeResult = request->Invoke()
        .ApplyUnique(std::move(rspHandler));
    auto metaHandlerResult = request->GetResponseAttachmentsStream()->Read()
        .Apply(std::move(metaHandler));
    return metaHandlerResult.Apply(BIND ([req = std::move(request), res = std::move(invokeResult)] () mutable {
        return NDetail::CreateRpcClientOutputStreamFromInvokedRequest(
            std::move(req),
            std::move(res));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

