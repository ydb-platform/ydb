#include "server.h"

#include <yt/yt/core/rpc/server_detail.h>
#include <yt/yt/core/rpc/private.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NRpc::NBus {

using namespace NConcurrency;
using namespace NYT::NBus;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TBusServer
    : public TServerBase
    , public IMessageHandler
{
public:
    explicit TBusServer(IBusServerPtr busServer)
        : TServerBase(RpcServerLogger().WithTag("BusServerId: %v", TGuid::Create()))
        , BusServer_(std::move(busServer))
    { }

private:
    IBusServerPtr BusServer_;


    void DoStart() override
    {
        BusServer_->Start(this);
        TServerBase::DoStart();
    }

    TFuture<void> DoStop(bool graceful) override
    {
        return TServerBase::DoStop(graceful)
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                // NB: Stop the bus server anyway.
                auto future = BusServer_->Stop();
                BusServer_.Reset();
                error.ThrowOnError();
                return future;
            }));
    }

    void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override
    {
        auto messageType = GetMessageType(message);
        switch (messageType) {
            case EMessageType::Request:
                OnRequestMessage(std::move(message), std::move(replyBus));
                break;

            case EMessageType::RequestCancelation:
                OnRequestCancelationMessage(std::move(message));
                break;

            case EMessageType::StreamingPayload:
                OnStreamingPayloadMessage(std::move(message));
                break;

            case EMessageType::StreamingFeedback:
                OnStreamingFeedbackMessage(std::move(message));
                break;

            default:
                // Unable to reply, no request id is known.
                // Let's just drop the message.
                YT_LOG_ERROR("Incoming message has invalid type, ignored (Type: %x)",
                    static_cast<ui32>(messageType));
                break;
        }
    }

    void OnRequestMessage(TSharedRefArray message, IBusPtr replyBus)
    {
        auto header = std::make_unique<NProto::TRequestHeader>();
        if (!TryParseRequestHeader(message, header.get())) {
            // Unable to reply, no request id is known.
            // Let's just drop the message.
            YT_LOG_ERROR("Error parsing request header");
            return;
        }

        auto requestId = FromProto<TRequestId>(header->request_id());
        const auto& serviceName = header->service();
        auto realmId = FromProto<TRealmId>(header->realm_id());
        auto tosLevel = header->tos_level();

        if (message.Size() < 2) {
            YT_LOG_ERROR("Too few request parts: expected >= 2, actual %v (RequestId: %v)",
                message.Size(),
                requestId);
            return;
        }

        YT_LOG_DEBUG("Request received (RequestId: %v, Endpoint: %v)",
            requestId,
            replyBus->GetEndpointDescription());

        auto replyWithError = [&] (const TError& error) {
            YT_LOG_DEBUG(error);
            auto response = CreateErrorResponseMessage(requestId, error);
            YT_UNUSED_FUTURE(replyBus->Send(std::move(response)));
        };

        if (!Started_) {
            replyWithError(TError(
                NRpc::EErrorCode::Unavailable,
                "Server is not started")
                << TErrorAttribute("realm_id", realmId)
                << TErrorAttribute("endpoint", replyBus->GetEndpointDescription()));
            return;
        }

        TServiceId serviceId(serviceName, realmId);
        IServicePtr service;
        try {
            service = GetServiceOrThrow(serviceId);
        } catch (const TErrorException& ex) {
            replyWithError(TError(ex));
            return;
        }

        replyBus->SetTosLevel(tosLevel);

        YT_VERIFY(service);
        service->HandleRequest(
            std::move(header),
            std::move(message),
            std::move(replyBus));
    }

    void OnRequestCancelationMessage(TSharedRefArray message)
    {
        NProto::TRequestCancelationHeader header;
        if (!TryParseRequestCancelationHeader(message, &header)) {
            // Unable to reply, no request id is known.
            // Let's just drop the message.
            YT_LOG_ERROR("Error parsing request cancelation header");
            return;
        }

        auto requestId = FromProto<TRequestId>(header.request_id());
        const auto& serviceName = header.service();
        const auto& methodName = header.method();
        auto realmId = FromProto<TRealmId>(header.realm_id());

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            YT_LOG_DEBUG("Service is not registered (Service: %v, RealmId: %v, RequestId: %v)",
                serviceName,
                realmId,
                requestId);
            return;
        }

        YT_LOG_DEBUG("Request cancelation received (Method: %v.%v, RealmId: %v, RequestId: %v)",
            serviceName,
            methodName,
            realmId,
            requestId);

        service->HandleRequestCancellation(requestId);
    }

    void OnStreamingPayloadMessage(TSharedRefArray message)
    {
        NProto::TStreamingPayloadHeader header;
        if (!TryParseStreamingPayloadHeader(message, &header)) {
            // Unable to reply, no request id is known.
            // Let's just drop the message.
            YT_LOG_ERROR("Error parsing request streaming payload header");
            return;
        }

        auto requestId = FromProto<TRequestId>(header.request_id());
        auto sequenceNumber = header.sequence_number();
        auto attachments = std::vector<TSharedRef>(message.Begin() + 1, message.End());
        const auto& serviceName = header.service();
        auto realmId = FromProto<TRealmId>(header.realm_id());

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            YT_LOG_DEBUG("Service is not registered (Service: %v, RealmId: %v, RequestId: %v)",
                serviceName,
                realmId,
                requestId);
            return;
        }

        if (attachments.empty()) {
            YT_LOG_WARNING("Streaming payload without attachments; canceling request (RequestId: %v)",
                requestId);
            service->HandleRequestCancellation(requestId);
            return;
        }

        NCompression::ECodec codec;
        int intCodec = header.codec();
        if (!TryEnumCast(intCodec, &codec)) {
            YT_LOG_WARNING("Streaming payload codec is not supported; canceling request (RequestId: %v, Codec: %v)",
                requestId,
                intCodec);
            service->HandleRequestCancellation(requestId);
            return;
        }

        YT_LOG_DEBUG("Request streaming payload received (RequestId: %v, SequenceNumber: %v, Sizes: %v, "
            "Codec: %v, Closed: %v)",
            requestId,
            sequenceNumber,
            MakeFormattableView(attachments, [] (auto* builder, const auto& attachment) {
                builder->AppendFormat("%v", GetStreamingAttachmentSize(attachment));
            }),
            codec,
            !attachments.back());

        TStreamingPayload payload{
            codec,
            sequenceNumber,
            std::move(attachments)
        };
        service->HandleStreamingPayload(requestId, payload);
    }

    void OnStreamingFeedbackMessage(TSharedRefArray message)
    {
        NProto::TStreamingFeedbackHeader header;
        if (!TryParseStreamingFeedbackHeader(message, &header)) {
            // Unable to reply, no request id is known.
            // Let's just drop the message.
            YT_LOG_ERROR("Error parsing request streaming feedback header");
            return;
        }

        auto requestId = FromProto<TRequestId>(header.request_id());
        auto readPosition = header.read_position();
        auto attachments = std::vector<TSharedRef>(message.Begin() + 1, message.End());
        const auto& serviceName = header.service();
        auto realmId = FromProto<TRealmId>(header.realm_id());

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            YT_LOG_DEBUG("Service is not registered (Service: %v, RealmId: %v, RequestId: %v)",
                serviceName,
                realmId,
                requestId);
            return;
        }

        YT_LOG_DEBUG("Request streaming feedback received (RequestId: %v, ReadPosition: %v)",
            requestId,
            readPosition);

        TStreamingFeedback feedback{
            readPosition
        };
        service->HandleStreamingFeedback(requestId, feedback);
    }
};

IServerPtr CreateBusServer(NBus::IBusServerPtr busServer)
{
    return New<TBusServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NBus
