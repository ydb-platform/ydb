#include "local_channel.h"
#include "channel.h"
#include "client.h"
#include "message.h"
#include "server.h"
#include "service.h"
#include "dispatcher.h"
#include "private.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/net/address.h>

#include <atomic>

namespace NYT::NRpc {

using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NBus;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

static const TString EndpointDescription = "<local>";
static const IAttributeDictionaryPtr EndpointAttributes =
    ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("local").Value(true)
        .EndMap());

////////////////////////////////////////////////////////////////////////////////

class TLocalChannel
    : public IChannel
{
public:
    explicit TLocalChannel(IServerPtr server)
        : Server_(std::move(server))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes;
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        TServiceId serviceId(request->GetService(), request->GetRealmId());
        IServicePtr service;
        try {
            service = Server_->GetServiceOrThrow(serviceId);
        } catch (const TErrorException& ex) {
            responseHandler->HandleError(TError(ex));
            return nullptr;
        }

        auto& header = request->Header();
        header.set_start_time(ToProto<i64>(TInstant::Now()));
        if (options.Timeout) {
            header.set_timeout(ToProto<i64>(*options.Timeout));
        } else {
            header.clear_timeout();
        }

        TSharedRefArray serializedRequest;
        try {
            serializedRequest = request->Serialize();
        } catch (const std::exception& ex) {
            responseHandler->HandleError(TError(NRpc::EErrorCode::TransportError, "Request serialization failed")
                << *EndpointAttributes
                << TErrorAttribute("request_id", request->GetRequestId())
                << ex);
            return nullptr;
        }

        auto session = New<TSession>(
            request->GetRequestId(),
            std::move(responseHandler),
            options.Timeout);

        YT_LOG_DEBUG("Local request sent (RequestId: %v, Method: %v.%v, Timeout: %v)",
            request->GetRequestId(),
            request->GetService(),
            request->GetMethod(),
            options.Timeout);

        service->HandleRequest(
            std::make_unique<NProto::TRequestHeader>(request->Header()),
            std::move(serializedRequest),
            std::move(session));

        return New<TClientRequestControl>(std::move(service), request->GetRequestId());
    }

    void Terminate(const TError& error) override
    {
        Terminated_.Fire(error);
    }

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        Terminated_.Subscribe(callback);
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        Terminated_.Unsubscribe(callback);
    }

    int GetInflightRequestCount() override
    {
        return 0;
    }

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override
    {
        return MemoryUsageTracker_;
    }

private:
    class TSession;
    using TSessionPtr = TIntrusivePtr<TSession>;

    const IServerPtr Server_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_ = GetNullMemoryUsageTracker();

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    class TSession
        : public IBus
    {
    public:
        TSession(
            TRequestId requestId,
            IClientResponseHandlerPtr handler,
            std::optional<TDuration> timeout)
            : RequestId_(requestId)
            , Handler_(std::move(handler))
        {
            if (timeout) {
                TDelayedExecutor::Submit(
                    BIND(&TSession::OnTimeout, MakeStrong(this)),
                    *timeout);
            }
        }

        const TString& GetEndpointDescription() const override
        {
            return EndpointDescription;
        }

        const IAttributeDictionary& GetEndpointAttributes() const override
        {
            return *EndpointAttributes;
        }

        TBusNetworkStatistics GetNetworkStatistics() const override
        {
            return {};
        }

        const TString& GetEndpointAddress() const override
        {
            static const TString EmptyAddress;
            return EmptyAddress;
        }

        bool IsEndpointLocal() const override
        {
            return true;
        }

        bool IsEncrypted() const override
        {
            return false;
        }

        const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override
        {
            return NNet::NullNetworkAddress;
        }

        TFuture<void> GetReadyFuture() const override
        {
            return VoidFuture;
        }

        TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            auto messageType = GetMessageType(message);
            switch (messageType) {
                case EMessageType::Response:
                    OnResponseMessage(std::move(message));
                    break;

                case EMessageType::StreamingPayload:
                    OnStreamingPayloadMessage(std::move(message));
                    break;

                case EMessageType::StreamingFeedback:
                    OnStreamingFeedbackMessage(std::move(message));
                    break;

                default:
                    YT_ABORT();
            }
            return VoidFuture;
        }

        void SetTosLevel(TTosLevel /*tosLevel*/) override
        { }

        void Terminate(const TError& /*error*/) override
        { }

        void SubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
        { }

        void UnsubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
        { }

    private:
        const TRequestId RequestId_;
        const IClientResponseHandlerPtr Handler_;

        std::atomic<bool> Replied_ = false;

        void OnResponseMessage(TSharedRefArray message)
        {
            NProto::TResponseHeader header;
            YT_VERIFY(TryParseResponseHeader(message, &header));
            if (AcquireLock()) {
                auto error = FromProto<TError>(header.error());
                if (error.IsOK()) {
                    YT_LOG_DEBUG("Local response received (RequestId: %v)",
                        RequestId_);
                    Handler_->HandleResponse(std::move(message), /*address*/ {});
                } else {
                    ReportError(error);
                }
            }
        }

        void OnStreamingPayloadMessage(TSharedRefArray message)
        {
            NProto::TStreamingPayloadHeader header;
            YT_VERIFY(TryParseStreamingPayloadHeader(message, &header));

            auto sequenceNumber = header.sequence_number();
            auto attachments = std::vector<TSharedRef>(message.Begin() + 1, message.End());

            YT_VERIFY(!attachments.empty());

            NCompression::ECodec codec;
            int intCodec = header.codec();
            YT_VERIFY(TryEnumCast(intCodec, &codec));

            YT_LOG_DEBUG("Response streaming payload received (RequestId: %v, SequenceNumber: %v, Sizes: %v, "
                "Codec: %v, Closed: %v)",
                RequestId_,
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
            Handler_->HandleStreamingPayload(payload);
        }

        void OnStreamingFeedbackMessage(TSharedRefArray message)
        {
            NProto::TStreamingFeedbackHeader header;
            YT_VERIFY(TryParseStreamingFeedbackHeader(message, &header));
            auto readPosition = header.read_position();

            YT_LOG_DEBUG("Response streaming feedback received (RequestId: %v, ReadPosition: %v)",
                RequestId_,
                readPosition);

            TStreamingFeedback feedback{
                readPosition
            };
            Handler_->HandleStreamingFeedback(feedback);
        }

        bool AcquireLock()
        {
            return !Replied_.exchange(true);
        }

        void OnTimeout(bool aborted)
        {
            if (!AcquireLock()) {
                return;
            }

            ReportError(aborted
                ? TError(NYT::EErrorCode::Canceled, "Request timed out (timer was aborted)")
                : TError(NYT::EErrorCode::Timeout, "Request timed out"));
        }

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("request_id", RequestId_)
                << GetEndpointAttributes();

            YT_LOG_DEBUG(detailedError, "Local request failed (RequestId: %v)",
                RequestId_);

            Handler_->HandleError(std::move(detailedError));
        }
    };

    class TClientRequestControl
        : public IClientRequestControl
    {
    public:
        TClientRequestControl(IServicePtr service, TRequestId requestId)
            : Service_(std::move(service))
            , RequestId_(requestId)
        { }

        void Cancel() override
        {
            Service_->HandleRequestCancellation(RequestId_);
        }

        TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override
        {
            Service_->HandleStreamingPayload(RequestId_, payload);
            return VoidFuture;
        }

        TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override
        {
            Service_->HandleStreamingFeedback(RequestId_, feedback);
            return VoidFuture;
        }

    private:
        const IServicePtr Service_;
        const TRequestId RequestId_;

    };
};

IChannelPtr CreateLocalChannel(IServerPtr server)
{
    return New<TLocalChannel>(server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
