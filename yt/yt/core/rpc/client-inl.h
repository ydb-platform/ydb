#ifndef CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include client.h"
// For the sake of sane code completion.
#include "client.h"
#endif

#include "helpers.h"
#include "stream.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class E>
TServiceDescriptor& TServiceDescriptor::SetFeaturesType()
{
    static const std::function<std::optional<TStringBuf>(int featureId)> Formatter = [] (int featureId) {
        return TEnumTraits<E>::FindLiteralByValue(static_cast<E>(featureId));
    };
    FeatureIdFormatter = &Formatter;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class E>
void IClientRequest::DeclareClientFeature(E featureId)
{
    DeclareClientFeature(FeatureIdToInt(featureId));
}

template <class E>
void IClientRequest::RequireServerFeature(E featureId)
{
    RequireServerFeature(FeatureIdToInt(featureId));
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
TTypedClientRequest<TRequestMessage, TResponse>::TTypedClientRequest(
    IChannelPtr channel,
    const TServiceDescriptor& serviceDescriptor,
    const TMethodDescriptor& methodDescriptor)
    : TClientRequest(
        std::move(channel),
        serviceDescriptor,
        methodDescriptor)
{ }

template <class TRequestMessage, class TResponse>
TFuture<typename TResponse::TResult> TTypedClientRequest<TRequestMessage, TResponse>::Invoke()
{
    auto context = CreateClientContext();
    auto requestAttachmentsStream = context->GetRequestAttachmentsStream();
    auto responseAttachmentsStream = context->GetResponseAttachmentsStream();

    typename TResponse::TResult response;
    {
        auto traceContextGuard = NTracing::TCurrentTraceContextGuard(context->GetTraceContext());
        response = New<TResponse>(std::move(context));
    }

    auto promise = response->GetPromise();
    auto requestControl = Send(std::move(response));
    if (requestControl) {
        auto subscribeToStreamAbort = [&] (const auto& stream) {
            if (stream) {
                stream->SubscribeAborted(BIND([=] {
                    requestControl->Cancel();
                }));
            }
        };
        subscribeToStreamAbort(requestAttachmentsStream);
        subscribeToStreamAbort(responseAttachmentsStream);
        promise.OnCanceled(BIND([=] (const TError& /*error*/) {
            requestControl->Cancel();
        }));
    }
    return promise.ToFuture();
}

template <class TRequestMessage, class TResponse>
TSharedRefArray TTypedClientRequest<TRequestMessage, TResponse>::SerializeHeaderless() const
{
    TSharedRefArrayBuilder builder(Attachments().size() + 1);

    // COMPAT(danilalexeev): legacy RPC codecs
    builder.Add(EnableLegacyRpcCodecs_
        ? SerializeProtoToRefWithEnvelope(*this, RequestCodec_, false)
        : SerializeProtoToRefWithCompression(*this, RequestCodec_, false));

    auto attachmentCodecId = EnableLegacyRpcCodecs_
        ? NCompression::ECodec::None
        : RequestCodec_;
    auto compressedAttachments = CompressAttachments(Attachments(), attachmentCodecId);
    for (auto&& attachment : compressedAttachments) {
        builder.Add(std::move(attachment));
    }

    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
TTypedClientResponse<TResponseMessage>::TTypedClientResponse(TClientContextPtr clientContext)
    : TClientResponse(std::move(clientContext))
{ }

template <class TResponseMessage>
auto TTypedClientResponse<TResponseMessage>::GetPromise() -> TPromise<TResult>
{
    return Promise_;
}

template <class TResponseMessage>
void TTypedClientResponse<TResponseMessage>::SetPromise(const TError& error)
{
    if (error.IsOK()) {
        Promise_.Set(this);
    } else {
        Promise_.Set(error);
    }
    Promise_.Reset();
}

template <class TResponseMessage>
bool TTypedClientResponse<TResponseMessage>::TryDeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId)
{
    auto traceContextGuard = NTracing::TCurrentTraceContextGuard(ClientContext_->GetTraceContext());

    return codecId
        ? TryDeserializeProtoWithCompression(this, data, *codecId)
        // COMPAT(danilalexeev): legacy RPC codecs
        : TryDeserializeProtoWithEnvelope(this, data);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> TProxyBase::CreateRequest(const TMethodDescriptor& methodDescriptor) const
{
    auto request = New<T>(
        Channel_,
        ServiceDescriptor_,
        methodDescriptor);
    request->SetTimeout(DefaultTimeout_);
    request->SetAcknowledgementTimeout(DefaultAcknowledgementTimeout_);
    request->SetRequestCodec(DefaultRequestCodec_);
    request->SetResponseCodec(DefaultResponseCodec_);
    request->SetEnableLegacyRpcCodecs(DefaultEnableLegacyRpcCodecs_);
    request->SetMultiplexingBand(methodDescriptor.MultiplexingBand);

    if (methodDescriptor.StreamingEnabled) {
        request->ClientAttachmentsStreamingParameters() =
            DefaultClientAttachmentsStreamingParameters_;
        request->ServerAttachmentsStreamingParameters() =
            DefaultServerAttachmentsStreamingParameters_;
    }

    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
