#include "channel.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/stream.h>
#include <yt/yt/core/rpc/private.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/misc/tls.h>

#include <array>

namespace NYT::NRpc::NBus {

using namespace NYT::NBus;
using namespace NConcurrency;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TBusChannel
    : public IChannel
{
public:
    TBusChannel(
        IBusClientPtr client,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Client_(std::move(client))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        YT_VERIFY(Client_);
        YT_VERIFY(MemoryUsageTracker_);
    }

    const TString& GetEndpointDescription() const override
    {
        return Client_->GetEndpointDescription();
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Client_->GetEndpointAttributes();
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TSessionPtr session;

        try {
            session = GetOrCreateSession(options);
        } catch (const std::exception& ex) {
            responseHandler->HandleError(TError(ex));
            return nullptr;
        }

        return session->SendRequest(
            std::move(request),
            std::move(responseHandler),
            options);
    }

    void Terminate(const TError& error) override
    {
        YT_VERIFY(!error.IsOK());
        VERIFY_THREAD_AFFINITY_ANY();

        if (TerminationFlag_.exchange(true)) {
            return;
        }

        TerminationError_.Store(error);

        std::vector<TSessionPtr> sessions;
        for (auto& bucket : Buckets_) {
            auto guard = WriterGuard(bucket.Lock);

            for (auto&& session : bucket.Sessions) {
                sessions.push_back(std::move(session));
            }
            bucket.Sessions.clear();

            bucket.Terminated = true;
        }

        for (const auto& session : sessions) {
            session->Terminate(error);
        }

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
        int requestCount = 0;

        for (auto& bucket : Buckets_) {
            auto guard = ReaderGuard(bucket.Lock);
            for (const auto& session : bucket.Sessions) {
                requestCount += session->GetInflightRequestCount();
            }
        }

        return requestCount;
    }

    IMemoryUsageTrackerPtr GetChannelMemoryTracker() override
    {
        return MemoryUsageTracker_;
    }

private:
    class TSession;
    using TSessionPtr = TIntrusivePtr<TSession>;

    class TClientRequestControl;
    using TClientRequestControlPtr = TIntrusivePtr<TClientRequestControl>;

    const IBusClientPtr Client_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    struct TBandBucket
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
        std::atomic<size_t> CurrentSessionIndex = 0;
        std::vector<TSessionPtr> Sessions;
        bool Terminated = false;
    };

    TEnumIndexedArray<EMultiplexingBand, TBandBucket> Buckets_;

    std::atomic<bool> TerminationFlag_ = false;
    TAtomicObject<TError> TerminationError_;

    TSessionPtr GetOrCreateSession(const TSendOptions& options)
    {
        auto& bucket = Buckets_[options.MultiplexingBand];
        auto index = options.MultiplexingParallelism <= 1 ? 0 : bucket.CurrentSessionIndex++ % options.MultiplexingParallelism;

        // Fast path.
        {
            auto guard = ReaderGuard(bucket.Lock);
            if (bucket.Sessions.size() > index) {
                return bucket.Sessions[index];
            }
        }

        std::vector<std::pair<IBusPtr, TSessionPtr>> results;

        // Slow path.
        {
            auto guard = WriterGuard(bucket.Lock);

            if (bucket.Sessions.size() > index) {
                return bucket.Sessions[index];
            }

            if (bucket.Terminated) {
                guard.Release();
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransportError, "Channel terminated")
                    << TerminationError_.Load();
            }

            bucket.Sessions.reserve(options.MultiplexingParallelism);
            while (bucket.Sessions.size() <= index) {
                auto session = New<TSession>(
                    options.MultiplexingBand,
                    MemoryUsageTracker_);
                auto messageHandler = New<TMessageHandler>(session);

                auto bus = Client_->CreateBus(
                    messageHandler,
                    {
                        .MultiplexingBand = options.MultiplexingBand
                    });

                const auto& attrs = bus->GetEndpointAttributes();
                YT_LOG_DEBUG("Created bus (ConnectionType: Client, VerificationMode: %v, EncryptionMode: %v, Endpoint: %v)",
                    attrs.Get<EVerificationMode>("verification_mode"),
                    attrs.Get<EEncryptionMode>("encryption_mode"),
                    attrs.Get<TString>("address"));

                session->Initialize(bus);
                bucket.Sessions.push_back(session);
                results.emplace_back(std::move(bus), std::move(session));
            }
        }

        for (const auto& [bus, session] : results) {
            bus->SubscribeTerminated(BIND_NO_PROPAGATE(
                &TBusChannel::OnBusTerminated,
                MakeWeak(this),
                MakeWeak(session),
                options.MultiplexingBand));
        }

        return results.back().second;
    }

    void OnBusTerminated(const TWeakPtr<TSession>& session, EMultiplexingBand band, const TError& error)
    {
        auto session_ = session.Lock();
        if (!session_) {
            return;
        }

        auto& bucket = Buckets_[band];

        {
            auto guard = WriterGuard(bucket.Lock);

            for (size_t index = 0; index < bucket.Sessions.size(); ++index) {
                if (bucket.Sessions[index] == session_) {
                    std::swap(bucket.Sessions[index], bucket.Sessions.back());
                    bucket.Sessions.pop_back();
                    break;
                }
            }
        }

        session_->Terminate(error);
    }

    //! Provides a weak wrapper around a session and breaks the cycle
    //! between the session and its underlying bus.
    class TMessageHandler
        : public IMessageHandler
    {
    public:
        explicit TMessageHandler(TSessionPtr session)
            : Session_(std::move(session))
        { }

        void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override
        {
            auto session_ = Session_.Lock();
            if (session_) {
                session_->HandleMessage(std::move(message), std::move(replyBus));
            }
        }

    private:
        const TWeakPtr<TSession> Session_;
    };

    //! Directs requests sent via a channel to go through its underlying bus.
    //! Terminates when the underlying bus does so.
    class TSession
        : public IMessageHandler
    {
    public:
        TSession(
            EMultiplexingBand band,
            IMemoryUsageTrackerPtr memoryUsageTracker)
            : TosLevel_(TTcpDispatcher::Get()->GetTosLevelForBand(band))
            , MemoryUsageTracker_(std::move(memoryUsageTracker))
        {
            YT_VERIFY(MemoryUsageTracker_);
        }

        void Initialize(IBusPtr bus)
        {
            YT_ASSERT(bus);
            Bus_ = std::move(bus);
            Bus_->SetTosLevel(TosLevel_);
        }

        void Terminate(const TError& error)
        {
            YT_VERIFY(!error.IsOK());

            if (TerminationFlag_.exchange(true)) {
                return;
            }

            TerminationError_.Store(error);

            std::vector<std::tuple<TClientRequestControlPtr, IClientResponseHandlerPtr>> existingRequests;

            // Mark the channel as terminated to disallow any further usage.
            for (auto& bucket : RequestBuckets_) {
                auto guard = Guard(bucket);

                bucket.Terminated = true;

                existingRequests.reserve(bucket.ActiveRequestMap.size());
                for (auto& [requestId, requestControl] : bucket.ActiveRequestMap) {
                    auto responseHandler = requestControl->Finalize(guard);
                    existingRequests.emplace_back(std::move(requestControl), std::move(responseHandler));
                }

                bucket.ActiveRequestMap.clear();
            }

            for (const auto& existingRequest : existingRequests) {
                NotifyError(
                    std::get<0>(existingRequest),
                    std::get<1>(existingRequest),
                    TStringBuf("Request failed due to channel termination"),
                    error);
            }
        }

        IClientRequestControlPtr SendRequest(
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            const TSendOptions& options)
        {
            YT_VERIFY(request);
            YT_VERIFY(responseHandler);
            VERIFY_THREAD_AFFINITY_ANY();

            auto requestControl = New<TClientRequestControl>(
                this,
                request,
                options,
                std::move(responseHandler));

            {
                // NB: Requests without timeout are rare but may occur.
                // For these requests we still need to register a timeout cookie with TDelayedExecutor
                // since this also provides proper cleanup and cancelation when global shutdown happens.
                auto effectiveTimeout = options.Timeout.value_or(TDuration::Hours(24));
                auto timeoutCookie = TDelayedExecutor::Submit(
                    BIND(&TSession::HandleTimeout, MakeWeak(this), requestControl),
                    effectiveTimeout,
                    TDispatcher::Get()->GetHeavyInvoker());
                requestControl->SetTimeoutCookie(std::move(timeoutCookie));
            }

            if (auto readyFuture = GetBusReadyFuture()) {
                YT_LOG_DEBUG("Waiting for bus to become ready (RequestId: %v, Method: %v.%v)",
                    requestControl->GetRequestId(),
                    requestControl->GetService(),
                    requestControl->GetMethod());

                readyFuture.Subscribe(BIND(
                    [
                        =,
                        this,
                        this_ = MakeStrong(this),
                        request = std::move(request)
                    ] (const TError& error) {
                        if (!BusReady_.exchange(true)) {
                            YT_LOG_DEBUG(error, "Bus has become ready (Endpoint: %v)",
                                Bus_->GetEndpointDescription());
                        }
                        DoSendRequest(
                            std::move(request),
                            requestControl,
                            options);
                    })
                    .Via(TDispatcher::Get()->GetHeavyInvoker()));
            } else {
                DoSendRequest(
                    std::move(request),
                    requestControl,
                    options);
            }

            return requestControl;
        }

        YT_PREVENT_TLS_CACHING void Cancel(const TClientRequestControlPtr& requestControl)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            auto requestId = requestControl->GetRequestId();
            auto* bucket = GetBucketForRequest(requestId);

            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(*bucket);

                auto it = bucket->ActiveRequestMap.find(requestId);
                if (it == bucket->ActiveRequestMap.end()) {
                    YT_LOG_DEBUG("Attempt to cancel an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                if (requestControl != it->second) {
                    YT_LOG_DEBUG("Attempt to cancel a resent request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl->ProfileCancel();
                responseHandler = requestControl->Finalize(guard);
                bucket->ActiveRequestMap.erase(it);
            }

            // YT-1639: Avoid long chain of recursive calls.
            thread_local int Depth = 0;
            constexpr int MaxDepth = 10;
            if (Depth < MaxDepth) {
                ++Depth;
                NotifyError(
                    requestControl,
                    responseHandler,
                    TStringBuf("Request canceled"),
                    TError(NYT::EErrorCode::Canceled, "Request canceled"));
                --Depth;
            } else {
                TDispatcher::Get()->GetHeavyInvoker()->Invoke(BIND(
                    &TSession::NotifyError,
                    MakeStrong(this),
                    requestControl,
                    responseHandler,
                    TStringBuf("Request canceled"),
                    TError(NYT::EErrorCode::Canceled, "Request canceled")));
            }

            if (TerminationFlag_.load()) {
                return;
            }

            NProto::TRequestCancelationHeader header;
            ToProto(header.mutable_request_id(), requestId);
            ToProto(header.mutable_service(), requestControl->GetService());
            ToProto(header.mutable_method(), requestControl->GetMethod());
            if (auto realmId = requestControl->GetRealmId()) {
                ToProto(header.mutable_realm_id(), requestControl->GetRealmId());
            }

            auto message = CreateRequestCancelationMessage(header);
            YT_UNUSED_FUTURE(Bus_->Send(std::move(message)));
        }

        TFuture<void> SendStreamingPayload(
            const TClientRequestControlPtr& requestControl,
            const TStreamingPayload& payload)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (TerminationFlag_.load()) {
                return MakeFuture(TError(NRpc::EErrorCode::TransportError, "Session is terminated"));
            }

            NProto::TStreamingPayloadHeader header;
            ToProto(header.mutable_request_id(), requestControl->GetRequestId());
            ToProto(header.mutable_service(), requestControl->GetService());
            ToProto(header.mutable_method(), requestControl->GetMethod());
            if (auto realmId = requestControl->GetRealmId()) {
                ToProto(header.mutable_realm_id(), requestControl->GetRealmId());
            }
            header.set_sequence_number(payload.SequenceNumber);
            header.set_codec(static_cast<int>(payload.Codec));

            auto message = CreateStreamingPayloadMessage(header, payload.Attachments);
            NBus::TSendOptions options;
            options.TrackingLevel = EDeliveryTrackingLevel::Full;
            return Bus_->Send(std::move(message), options);
        }

        TFuture<void> SendStreamingFeedback(
            const TClientRequestControlPtr& requestControl,
            const TStreamingFeedback& feedback)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (TerminationFlag_.load()) {
                return MakeFuture(TError(NRpc::EErrorCode::TransportError, "Session is terminated"));
            }

            NProto::TStreamingFeedbackHeader header;
            ToProto(header.mutable_request_id(), requestControl->GetRequestId());
            ToProto(header.mutable_service(), requestControl->GetService());
            ToProto(header.mutable_method(), requestControl->GetMethod());
            if (auto realmId = requestControl->GetRealmId()) {
                ToProto(header.mutable_realm_id(), requestControl->GetRealmId());
            }
            header.set_read_position(feedback.ReadPosition);

            auto message = CreateStreamingFeedbackMessage(header);
            NBus::TSendOptions options;
            options.TrackingLevel = EDeliveryTrackingLevel::Full;
            return Bus_->Send(std::move(message), options);
        }

        void HandleTimeout(const TClientRequestControlPtr& requestControl, bool aborted)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            auto requestId = requestControl->GetRequestId();
            auto* bucket = GetBucketForRequest(requestId);

            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(*bucket);

                if (!requestControl->IsActive(guard)) {
                    return;
                }

                auto it = bucket->ActiveRequestMap.find(requestId);
                if (it != bucket->ActiveRequestMap.end() && requestControl == it->second) {
                    bucket->ActiveRequestMap.erase(it);
                } else {
                    YT_LOG_DEBUG("Timeout occurred for an unknown or resent request (RequestId: %v)",
                        requestId);
                }

                requestControl->ProfileTimeout();
                responseHandler = requestControl->Finalize(guard);
            }

            NotifyError(
                requestControl,
                responseHandler,
                TStringBuf("Request timed out"),
                TError(NYT::EErrorCode::Timeout, aborted
                    ? "Request timed out or timer was aborted"
                    : "Request timed out"));
        }

        void HandleAcknowledgementTimeout(const TClientRequestControlPtr& requestControl, bool aborted)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (aborted) {
                return;
            }

            auto requestId = requestControl->GetRequestId();
            auto* bucket = GetBucketForRequest(requestId);

            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(*bucket);

                if (!requestControl->IsActive(guard)) {
                    return;
                }

                auto it = bucket->ActiveRequestMap.find(requestId);
                if (it != bucket->ActiveRequestMap.end() && requestControl == it->second) {
                    bucket->ActiveRequestMap.erase(it);
                } else {
                    YT_LOG_DEBUG("Acknowledgement timeout occurred for an unknown or resent request (RequestId: %v)",
                        requestId);
                }

                requestControl->ProfileTimeout();
                responseHandler = requestControl->Finalize(guard);
            }

            auto error = TError(NYT::EErrorCode::Timeout, "Request acknowledgement timed out");

            NotifyError(
                requestControl,
                responseHandler,
                TStringBuf("Request acknowledgement timed out"),
                error);

            if (TerminationFlag_.load()) {
                return;
            }

            Bus_->Terminate(error);
        }

        void HandleMessage(TSharedRefArray message, IBusPtr /*replyBus*/) noexcept override
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
                    YT_LOG_ERROR("Incoming message has invalid type, ignored (Type: %x)",
                        static_cast<ui32>(messageType));
                    break;
            }
        }

        int GetInflightRequestCount()
        {
            int requestCount = 0;

            for (auto& bucket : RequestBuckets_) {
                requestCount += bucket.ActiveRequestCount.load(
                    std::memory_order::relaxed);
            }

            return requestCount;
        }

    private:
        const TTosLevel TosLevel_;
        const IMemoryUsageTrackerPtr MemoryUsageTracker_;

        IBusPtr Bus_;
        std::atomic<bool> BusReady_ = false;

        class TBucket
        {
        public:
            IBusPtr Bus;
            bool Terminated = false;
            THashMap<TRequestId, TClientRequestControlPtr> ActiveRequestMap;
            std::atomic<int> ActiveRequestCount = 0;

            void Acquire() noexcept
            {
                Lock.Acquire();
            }

            void Release() noexcept
            {
                if (std::ssize(ActiveRequestMap) != ActiveRequestCount.load(std::memory_order::relaxed)) {
                    ActiveRequestCount.store(std::ssize(ActiveRequestMap), std::memory_order::relaxed);
                }

                Lock.Release();
            }

        private:
            YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        };

        static constexpr size_t BucketCount = 64;

        std::array<TBucket, BucketCount> RequestBuckets_;

        std::atomic<bool> TerminationFlag_ = false;
        TAtomicObject<TError> TerminationError_;


        TFuture<void> GetBusReadyFuture()
        {
            if (Y_LIKELY(BusReady_.load(std::memory_order::relaxed))) {
                return {};
            }

            auto future = Bus_->GetReadyFuture();
            if (future.IsSet()) {
                BusReady_.store(true);
                return {};
            }

            return future;
        }

        void DoSendRequest(
            IClientRequestPtr request,
            TClientRequestControlPtr requestControl,
            const TSendOptions& options)
        {
            auto& header = request->Header();
            header.set_start_time(ToProto<i64>(TInstant::Now()));
            if (options.Timeout) {
                header.set_timeout(ToProto<i64>(*options.Timeout));
            } else {
                header.clear_timeout();
            }

            if (options.RequestHeavy) {
                BIND(&IClientRequest::Serialize, request)
                    .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
                    .Run()
                    .Subscribe(BIND(
                        &TSession::OnRequestSerialized,
                        MakeStrong(this),
                        std::move(requestControl),
                        options));
            } else {
                try {
                    auto requestMessage = request->Serialize();
                    OnRequestSerialized(
                        std::move(requestControl),
                        options,
                        std::move(requestMessage));
                } catch (const std::exception& ex) {
                    OnRequestSerialized(
                        std::move(requestControl),
                        options,
                        TError(ex));
                }
            }
        }


        TBucket* GetBucketForRequest(TRequestId requestId)
        {
            return &RequestBuckets_[requestId.Parts32[0] % BucketCount];
        }


        std::pair<IClientResponseHandlerPtr, NTracing::TCurrentTraceContextGuard> FindResponseHandlerAndTraceContextGuard(TRequestId requestId)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            auto* bucket = GetBucketForRequest(requestId);
            auto guard = Guard(*bucket);

            auto it = bucket->ActiveRequestMap.find(requestId);
            if (it == bucket->ActiveRequestMap.end()) {
                return {nullptr, NTracing::TCurrentTraceContextGuard(nullptr)};
            }

            const auto& requestControl = it->second;
            return {requestControl->GetResponseHandler(guard), requestControl->GetTraceContextGuard()};
        }


        void OnRequestSerialized(
            const TClientRequestControlPtr& requestControl,
            const TSendOptions& options,
            TErrorOr<TSharedRefArray> requestMessageOrError)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (requestMessageOrError.IsOK()) {
                auto requestMessageError = CheckBusMessageLimits(requestMessageOrError.Value());
                if (!requestMessageError.IsOK()){
                    requestMessageOrError = requestMessageError;
                }
            }

            auto requestId = requestControl->GetRequestId();
            auto* bucket = GetBucketForRequest(requestId);

            TClientRequestControlPtr existingRequestControl;
            IClientResponseHandlerPtr existingResponseHandler;
            {
                auto guard = Guard(*bucket);

                if (!requestControl->IsActive(guard)) {
                    return;
                }

                if (!requestMessageOrError.IsOK()) {
                    auto responseHandler = requestControl->Finalize(guard);
                    guard.Release();

                    NotifyError(
                        requestControl,
                        responseHandler,
                        TStringBuf("Request serialization failed"),
                        TError(NRpc::EErrorCode::TransportError, "Request serialization failed")
                            << requestMessageOrError);
                    return;
                }

                if (bucket->Terminated) {
                    auto responseHandler = requestControl->Finalize(guard);
                    guard.Release();

                    NotifyError(
                        requestControl,
                        responseHandler,
                        TStringBuf("Request is dropped because channel is terminated"),
                        TError(NRpc::EErrorCode::TransportError, "Channel terminated")
                            << TerminationError_.Load());
                    return;
                }

                // NB: We're OK with duplicate request ids.
                auto [it, inserted] = bucket->ActiveRequestMap.emplace(requestId, requestControl);
                if (!inserted) {
                    existingRequestControl = std::move(it->second);
                    existingResponseHandler = existingRequestControl->Finalize(guard);
                    it->second = requestControl;
                }

                if (options.AcknowledgementTimeout) {
                    auto timeoutCookie = TDelayedExecutor::Submit(
                        BIND(&TSession::HandleAcknowledgementTimeout, MakeWeak(this), requestControl),
                        *options.AcknowledgementTimeout,
                        TDispatcher::Get()->GetHeavyInvoker());
                    requestControl->SetAcknowledgementTimeoutCookie(std::move(timeoutCookie));
                }
            }

            if (existingResponseHandler) {
                NotifyError(
                    existingRequestControl,
                    existingResponseHandler,
                    "Request resent",
                    TError(NRpc::EErrorCode::TransportError, "Request resent"));
            }

            if (options.SendDelay) {
                Sleep(*options.SendDelay);
            }

            const auto& requestMessage = requestMessageOrError.Value();

            NBus::TSendOptions busOptions;
            busOptions.TrackingLevel = options.AcknowledgementTimeout
                ? EDeliveryTrackingLevel::Full
                : EDeliveryTrackingLevel::ErrorOnly;
            busOptions.ChecksummedPartCount = options.GenerateAttachmentChecksums
                ? NBus::TSendOptions::AllParts
                : 2; // RPC header + request body
            Bus_->Send(requestMessage, busOptions).Subscribe(BIND(
                &TSession::OnAcknowledgement,
                MakeStrong(this),
                options.AcknowledgementTimeout.has_value(),
                requestId));

            requestControl->ProfileRequest(requestMessage);

            YT_LOG_DEBUG("Request sent (RequestId: %v, Method: %v.%v, Timeout: %v, TrackingLevel: %v, "
                "ChecksummedPartCount: %v, MultiplexingBand: %v, Endpoint: %v, BodySize: %v, AttachmentsSize: %v)",
                requestId,
                requestControl->GetService(),
                requestControl->GetMethod(),
                requestControl->GetTimeout(),
                busOptions.TrackingLevel,
                busOptions.ChecksummedPartCount,
                options.MultiplexingBand,
                Bus_->GetEndpointDescription(),
                GetMessageBodySize(requestMessage),
                GetTotalMessageAttachmentSize(requestMessage));
        }


        void OnResponseMessage(TSharedRefArray message)
        {
            NProto::TResponseHeader header;
            if (!TryParseResponseHeader(message, &header)) {
                YT_LOG_ERROR("Error parsing response header");
                return;
            }

            auto requestId = FromProto<TRequestId>(header.request_id());
            auto* bucket = GetBucketForRequest(requestId);

            TClientRequestControlPtr requestControl;
            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(*bucket);

                if (bucket->Terminated) {
                    YT_LOG_WARNING("Response received via a terminated channel (RequestId: %v)",
                        requestId);
                    return;
                }

                auto it = bucket->ActiveRequestMap.find(requestId);
                if (it == bucket->ActiveRequestMap.end()) {
                    // This may happen when the other party responds to an already timed-out request.
                    YT_LOG_DEBUG("Response for an incorrect or obsolete request received (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl = std::move(it->second);
                requestControl->ProfileReply(message);
                responseHandler = requestControl->Finalize(guard);
                bucket->ActiveRequestMap.erase(it);
            }

            const auto traceContextGuard = requestControl->GetTraceContextGuard();

            {
                TError error;
                if (header.has_error()) {
                    error = FromProto<TError>(header.error());
                }
                if (error.IsOK()) {
                    message = TrackMemory(MemoryUsageTracker_, std::move(message));
                    if (MemoryUsageTracker_->IsExceeded()) {
                        auto error = TError(
                            NRpc::EErrorCode::MemoryPressure,
                            "Response is dropped due to high memory pressure");
                        requestControl->ProfileError(error);
                        NotifyError(
                            requestControl,
                            responseHandler,
                            TStringBuf("Response is dropped due to high memory pressure"),
                            error);
                    } else {
                        NotifyResponse(
                            requestId,
                            requestControl,
                            responseHandler,
                            std::move(message));
                    }
                } else {
                    requestControl->ProfileError(error);
                    if (error.GetCode() == EErrorCode::PoisonPill) {
                        YT_LOG_FATAL(error, "Poison pill received");
                    }
                    NotifyError(
                        requestControl,
                        responseHandler,
                        TStringBuf("Request failed"),
                        error);
                }
            }
        }

        void OnStreamingPayloadMessage(TSharedRefArray message)
        {
            NProto::TStreamingPayloadHeader header;
            if (!TryParseStreamingPayloadHeader(message, &header)) {
                YT_LOG_ERROR("Error parsing streaming payload header");
                return;
            }

            auto requestId = FromProto<TRequestId>(header.request_id());
            auto sequenceNumber = header.sequence_number();
            auto attachments = std::vector<TSharedRef>(message.Begin() + 1, message.End());

            auto [responseHandler, traceContextGuard] = FindResponseHandlerAndTraceContextGuard(requestId);

            if (!responseHandler) {
                YT_LOG_ERROR("Received streaming payload for an unknown request; ignored (RequestId: %v)",
                    requestId);
                return;
            }

            if (attachments.empty()) {
                responseHandler->HandleError(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Streaming payload without attachments"));
                return;
            }

            NCompression::ECodec codec;
            int intCodec = header.codec();
            if (!TryEnumCast(intCodec, &codec)) {
                responseHandler->HandleError(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Streaming payload codec %v is not supported",
                    intCodec));
                return;
            }

            YT_LOG_DEBUG("Response streaming payload received (RequestId: %v, SequenceNumber: %v, Sizes: %v, "
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
            responseHandler->HandleStreamingPayload(payload);
        }

        void OnStreamingFeedbackMessage(TSharedRefArray message)
        {
            NProto::TStreamingFeedbackHeader header;
            if (!TryParseStreamingFeedbackHeader(message, &header)) {
                YT_LOG_ERROR("Error parsing streaming feedback header");
                return;
            }

            auto requestId = FromProto<TRequestId>(header.request_id());
            auto readPosition = header.read_position();

            auto [responseHandler, traceContextGuard] = FindResponseHandlerAndTraceContextGuard(requestId);

            if (!responseHandler) {
                YT_LOG_DEBUG("Received streaming feedback for an unknown request; ignored (RequestId: %v)",
                    requestId);
                return;
            }

            YT_LOG_DEBUG("Response streaming feedback received (RequestId: %v, ReadPosition: %v)",
                requestId,
                readPosition);

            TStreamingFeedback feedback{
                readPosition
            };
            responseHandler->HandleStreamingFeedback(feedback);
        }

        void OnAcknowledgement(bool requestAcknowledgement, TRequestId requestId, const TError& error)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (!requestAcknowledgement && error.IsOK()) {
                return;
            }

            auto* bucket = GetBucketForRequest(requestId);

            TClientRequestControlPtr requestControl;
            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(*bucket);

                auto it = bucket->ActiveRequestMap.find(requestId);
                if (it == bucket->ActiveRequestMap.end()) {
                    // This one may easily get the actual response before the acknowledgment.
                    YT_LOG_DEBUG(error, "Acknowledgment received for an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl = it->second;
                requestControl->ResetAcknowledgementTimeoutCookie();
                if (!error.IsOK()) {
                    responseHandler = requestControl->Finalize(guard);
                    bucket->ActiveRequestMap.erase(it);
                } else {
                    requestControl->ProfileAcknowledgement();
                    responseHandler = requestControl->GetResponseHandler(guard);
                }
            }

            if (error.IsOK()) {
                NotifyAcknowledgement(requestId, responseHandler);
            } else {
                NotifyError(
                    requestControl,
                    responseHandler,
                    TStringBuf("Request acknowledgment failed"),
                    TError(NRpc::EErrorCode::TransportError, "Request acknowledgment failed")
                        << error);
            }
        }

        void NotifyError(
            const TClientRequestControlPtr& requestControl,
            const IClientResponseHandlerPtr& responseHandler,
            TStringBuf reason,
            const TError& error) noexcept
        {
            YT_VERIFY(responseHandler);

            auto detailedError = error
                << TErrorAttribute("realm_id", requestControl->GetRealmId())
                << TErrorAttribute("service", requestControl->GetService())
                << TErrorAttribute("method", requestControl->GetMethod())
                << TErrorAttribute("request_id", requestControl->GetRequestId())
                << Bus_->GetEndpointAttributes();

            if (requestControl->GetTimeout()) {
                detailedError = detailedError
                    << TErrorAttribute("timeout", *requestControl->GetTimeout());
            }

            if (!detailedError.HasTracingAttributes()) {
                if (auto tracingAttributes = requestControl->GetTracingAttributes()) {
                    detailedError.SetTracingAttributes(*tracingAttributes);
                }
            }

            YT_LOG_DEBUG(detailedError, "%v (RequestId: %v)",
                reason,
                requestControl->GetRequestId());

            responseHandler->HandleError(std::move(detailedError));
        }

        void NotifyAcknowledgement(
            TRequestId requestId,
            const IClientResponseHandlerPtr& responseHandler) noexcept
        {
            YT_LOG_DEBUG("Request acknowledged (RequestId: %v)", requestId);

            responseHandler->HandleAcknowledgement();
        }

        void NotifyResponse(
            TRequestId requestId,
            const TClientRequestControlPtr& requestControl,
            const IClientResponseHandlerPtr& responseHandler,
            TSharedRefArray message) noexcept
        {
            YT_LOG_DEBUG("Response received (RequestId: %v, Method: %v.%v, TotalTime: %v, AttachmentsSize: %v)",
                requestId,
                requestControl->GetService(),
                requestControl->GetMethod(),
                requestControl->GetTotalTime(),
                GetTotalMessageAttachmentSize(message));

            responseHandler->HandleResponse(std::move(message), Bus_->GetEndpointAddress());
        }
    };

    //! Controls a sent request.
    class TClientRequestControl
        : public TClientRequestPerformanceProfiler
    {
    public:
        TClientRequestControl(
            TSessionPtr session,
            IClientRequestPtr request,
            const TSendOptions& options,
            IClientResponseHandlerPtr responseHandler)
            : TClientRequestPerformanceProfiler(request->GetService(), request->GetMethod())
            , Session_(std::move(session))
            , RealmId_(request->GetRealmId())
            , Service_(request->GetService())
            , Method_(request->GetMethod())
            , RequestId_(request->GetRequestId())
            , Options_(options)
            , ResponseHandler_(std::move(responseHandler))
            , TraceContext_()
        { }

        ~TClientRequestControl()
        {
            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
            TDelayedExecutor::CancelAndClear(AcknowledgementTimeoutCookie_);
        }

        TRealmId GetRealmId() const
        {
            return RealmId_;
        }

        std::string GetService() const
        {
            return Service_;
        }

        std::string GetMethod() const
        {
            return Method_;
        }

        TRequestId GetRequestId() const
        {
            return RequestId_;
        }

        std::optional<TDuration> GetTimeout() const
        {
            return Options_.Timeout;
        }

        TDuration GetTotalTime() const
        {
            return TotalTime_;
        }

        NTracing::TCurrentTraceContextGuard GetTraceContextGuard() const
        {
            return TraceContext_.MakeTraceContextGuard();
        }

        std::optional<TTracingAttributes> GetTracingAttributes() const
        {
            return TraceContext_.GetTracingAttributes();
        }

        template <typename TLock>
        bool IsActive(const TGuard<TLock>&) const
        {
            return static_cast<bool>(ResponseHandler_);
        }

        void SetTimeoutCookie(TDelayedExecutorCookie cookie)
        {
            YT_ASSERT(!TimeoutCookie_);
            TimeoutCookie_ = std::move(cookie);
        }

        void SetAcknowledgementTimeoutCookie(TDelayedExecutorCookie cookie)
        {
            YT_ASSERT(!AcknowledgementTimeoutCookie_);
            AcknowledgementTimeoutCookie_ = std::move(cookie);
        }

        void ResetAcknowledgementTimeoutCookie()
        {
            TDelayedExecutor::CancelAndClear(AcknowledgementTimeoutCookie_);
        }

        template <typename TLock>
        IClientResponseHandlerPtr GetResponseHandler(const TGuard<TLock>&)
        {
            return ResponseHandler_;
        }

        template <typename TLock>
        IClientResponseHandlerPtr Finalize(const TGuard<TLock>&)
        {
            TotalTime_ = ProfileComplete();
            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
            TDelayedExecutor::CancelAndClear(AcknowledgementTimeoutCookie_);
            return std::move(ResponseHandler_);
        }

        // IClientRequestControl overrides
        void Cancel() override
        {
            Session_->Cancel(this);
        }

        TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override
        {
            return Session_->SendStreamingPayload(this, payload);
        }

        TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override
        {
            return Session_->SendStreamingFeedback(this, feedback);
        }

    private:
        const TSessionPtr Session_;
        const TRealmId RealmId_;
        const std::string Service_;
        const std::string Method_;
        const TRequestId RequestId_;
        const TSendOptions Options_;

        TDelayedExecutorCookie TimeoutCookie_;
        TDelayedExecutorCookie AcknowledgementTimeoutCookie_;
        IClientResponseHandlerPtr ResponseHandler_;

        TDuration TotalTime_;

        NYT::NTracing::TTraceContextHandler TraceContext_;
    };
};

IChannelPtr CreateBusChannel(
    IBusClientPtr client,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    YT_VERIFY(client);
    YT_VERIFY(memoryUsageTracker);

    return New<TBusChannel>(
        std::move(client),
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

class TTcpBusChannelFactory
    : public IChannelFactory
{
public:
    TTcpBusChannelFactory(
        TBusConfigPtr config,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(ConvertToNode(std::move(config)))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        YT_VERIFY(MemoryUsageTracker_);
    }

    IChannelPtr CreateChannel(const TString& address) override
    {
        auto config = TBusClientConfig::CreateTcp(address);
        config->Load(Config_, /*postprocess*/ true, /*setDefaults*/ false);
        auto client = CreateBusClient(
            std::move(config),
            GetYTPacketTranscoderFactory(),
            MemoryUsageTracker_);
        return CreateBusChannel(
            std::move(client),
            MemoryUsageTracker_);
    }

private:
    const INodePtr Config_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
};

IChannelFactoryPtr CreateTcpBusChannelFactory(
    TBusConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    return New<TTcpBusChannelFactory>(
        std::move(config),
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

class TUdsBusChannelFactory
    : public IChannelFactory
{
public:
    TUdsBusChannelFactory(
        TBusConfigPtr config,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(ConvertToNode(std::move(config)))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        YT_VERIFY(MemoryUsageTracker_);
    }

    IChannelPtr CreateChannel(const TString& address) override
    {
        auto config = TBusClientConfig::CreateUds(address);
        config->Load(Config_, /*postprocess*/ true, /*setDefaults*/ false);
        auto client = CreateBusClient(
            std::move(config),
            GetYTPacketTranscoderFactory(),
            MemoryUsageTracker_);
        return CreateBusChannel(
            std::move(client),
            MemoryUsageTracker_);
    }

private:
    const INodePtr Config_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
};

IChannelFactoryPtr CreateUdsBusChannelFactory(
    TBusConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    return New<TUdsBusChannelFactory>(
        std::move(config),
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NBus
