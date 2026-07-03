#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/errors.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/transaction.h>

#include <util/generic/size_literals.h>
#include <util/thread/pool.h>
#include <util/system/types.h>
#include <util/datetime/base.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

#include <google/protobuf/wire_format_lite.h>

#include <queue>
#include <condition_variable>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NTopic {

ERetryErrorClass GetRetryErrorClass(EStatus status);
ERetryErrorClass GetRetryErrorClassV2(EStatus status);

void Cancel(NYdbGrpc::IQueueClientContextPtr& context);

NYdb::NIssue::TIssues MakeIssueWithSubIssues(const std::string& description, const NYdb::NIssue::TIssues& subissues);

std::string IssuesSingleLineString(const NYdb::NIssue::TIssues& issues);

namespace NWriteSessionGrpc {

inline constexpr std::string_view PARTITION_KEY_META_KEY = "__partition_key";

inline size_t GetMaxGrpcMessageSize() {
    return 120_MB;
}

using TWireFormatLite = google::protobuf::internal::WireFormatLite;

inline size_t ProtoInt32FieldSize(ui32 fieldNumber, i32 value) {
    if (value == 0) {
        return 0;
    }
    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_INT32)
        + TWireFormatLite::Int32Size(value);
}

inline size_t ProtoInt64FieldSize(ui32 fieldNumber, i64 value) {
    if (value == 0) {
        return 0;
    }
    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_INT64)
        + TWireFormatLite::Int64Size(value);
}

inline size_t ProtoInt64FieldSizeUpperBound(ui32 fieldNumber) {
    static constexpr size_t MaxInt64VarintPayloadSize = 10;

    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_INT64)
        + MaxInt64VarintPayloadSize;
}

inline size_t ProtoPackedInt64FieldSize(ui32 fieldNumber, size_t dataSize) {
    if (dataSize == 0) {
        return 0;
    }
    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_INT64)
        + TWireFormatLite::LengthDelimitedSize(dataSize);
}

inline size_t ProtoBytesFieldSize(ui32 fieldNumber, size_t size) {
    if (size == 0) {
        return 0;
    }
    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_BYTES)
        + TWireFormatLite::LengthDelimitedSize(size);
}

inline size_t ProtoStringFieldSize(ui32 fieldNumber, size_t size) {
    if (size == 0) {
        return 0;
    }
    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_STRING)
        + TWireFormatLite::LengthDelimitedSize(size);
}

inline size_t ProtoMessageFieldSize(ui32 fieldNumber, size_t size) {
    return TWireFormatLite::TagSize(fieldNumber, TWireFormatLite::TYPE_MESSAGE)
        + TWireFormatLite::LengthDelimitedSize(size);
}

class TRequestSizeLimiter {
public:
    explicit TRequestSizeLimiter(ui32 envelopeFieldNumber, size_t maxSize = GetMaxGrpcMessageSize())
        : EnvelopeFieldNumber(envelopeFieldNumber)
        , MaxSize(maxSize)
    {
    }

    bool Empty() const {
        return BodySize == 0;
    }

    bool CanAdd(size_t deltaSize) const {
        return Empty() || ProtoMessageFieldSize(EnvelopeFieldNumber, BodySize + deltaSize) <= MaxSize;
    }

    void Add(size_t deltaSize) {
        BodySize += deltaSize;
    }

private:
    size_t BodySize = 0;
    ui32 EnvelopeFieldNumber;
    size_t MaxSize;
};

inline size_t ProtoTimestampFieldSize(ui32 fieldNumber, TInstant timestamp) {
    const ui64 milliseconds = timestamp.MilliSeconds();
    const i64 seconds = milliseconds / 1000;
    const i32 nanos = (milliseconds % 1000) * 1000000;
    const size_t timestampSize = ProtoInt64FieldSize(1, seconds)
        + ProtoInt32FieldSize(2, nanos);
    return ProtoMessageFieldSize(fieldNumber, timestampSize);
}

inline size_t ProtoMetadataItemFieldSize(ui32 fieldNumber, const std::pair<std::string, std::string>& item) {
    const size_t itemSize = ProtoStringFieldSize(1, item.first.size())
        + ProtoBytesFieldSize(2, item.second.size());
    return ProtoMessageFieldSize(fieldNumber, itemSize);
}

inline size_t ProtoTransactionIdentityFieldSize(ui32 fieldNumber, const std::optional<TTransactionId>& tx) {
    if (!tx) {
        return 0;
    }
    const size_t txSize = ProtoStringFieldSize(1, tx->TxId.size())
        + ProtoStringFieldSize(2, tx->SessionId.size());
    return ProtoMessageFieldSize(fieldNumber, txSize);
}

inline size_t ProtoTopicMessageDataFieldSize(
        TInstant createdAt,
        size_t dataSize,
        size_t uncompressedSize,
        size_t metadataSize) {
    const size_t messageDataSize = ProtoInt64FieldSizeUpperBound(1)
        + ProtoTimestampFieldSize(2, createdAt)
        + ProtoBytesFieldSize(3, dataSize)
        + ProtoInt64FieldSize(4, uncompressedSize)
        + metadataSize;
    return ProtoMessageFieldSize(1, messageDataSize);
}

template <typename TBlock, typename TOriginalMessages>
size_t EstimateTopicWriteRequestBlockSize(
        const TBlock& block,
        const TOriginalMessages& originalMessages,
        bool includeRequestFields) {
    Y_ABORT_UNLESS(!originalMessages.empty());

    size_t size = 0;
    if (includeRequestFields) {
        size += ProtoInt32FieldSize(2, static_cast<i32>(block.CodecID));
        size += ProtoTransactionIdentityFieldSize(3, originalMessages.front().Tx);
    }

    if (block.MessageCount > 1) {
        size_t metadataSize = 0;
        auto message = originalMessages.begin();
        const TInstant firstMessageCreatedAt = message->CreatedAt;
        for (const auto& item : message->MessageMeta) {
            metadataSize += ProtoMetadataItemFieldSize(7, item);
        }
        ++message;
        for (size_t i = 1; i < block.MessageCount; ++i) {
            Y_ABORT_UNLESS(message != originalMessages.end());
            for (const auto& item : message->MessageMeta) {
                if (item.first == PARTITION_KEY_META_KEY) {
                    metadataSize += ProtoMetadataItemFieldSize(7, item);
                }
            }
            ++message;
        }
        return size + ProtoTopicMessageDataFieldSize(firstMessageCreatedAt, block.Data.size(), block.OriginalSize, metadataSize);
    }

    const auto& message = originalMessages.front();
    size_t metadataSize = 0;
    for (const auto& item : message.MessageMeta) {
        metadataSize += ProtoMetadataItemFieldSize(7, item);
    }

    if (block.Compressed) {
        return size + ProtoTopicMessageDataFieldSize(message.CreatedAt, block.Data.size(), block.OriginalSize, metadataSize);
    }

    for (const auto& buffer : block.OriginalDataRefs) {
        size += ProtoTopicMessageDataFieldSize(message.CreatedAt, buffer.size(), block.OriginalSize, metadataSize);
    }
    return size;
}

} // namespace NWriteSessionGrpc

template <typename TEvent>
size_t CalcDataSize(const typename TEvent::TEvent& event) {
    constexpr bool UseMigrationProtocol = !std::is_same_v<TEvent, TEvent>;

    if (const typename TEvent::TDataReceivedEvent* dataEvent =
            std::get_if<typename TEvent::TDataReceivedEvent>(&event)) {
        size_t len = 0;

        bool hasCompressedMsgs = [&dataEvent](){
            if constexpr (UseMigrationProtocol) {
                return dataEvent->IsCompressedMessages();
            } else {
                return dataEvent->HasCompressedMessages();
            }
        }();

        if (hasCompressedMsgs) {
            for (const auto& msg : dataEvent->GetCompressedMessages()) {
                len += msg.GetData().size();
            }
        } else {
            for (const auto& msg : dataEvent->GetMessages()) {
                if (!msg.HasException()) {
                    len += msg.GetData().size();
                }
            }
        }
        return len;
    }
    return 0;
}

template <class TMessage>
bool IsErrorMessage(const TMessage& serverMessage) {
    const Ydb::StatusIds::StatusCode status = serverMessage.status();
    return status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
}

template <class TMessage>
TPlainStatus MakeErrorFromProto(const TMessage& serverMessage) {
    NYdb::NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(serverMessage.issues(), issues);
    return TPlainStatus(static_cast<EStatus>(serverMessage.status()), std::move(issues));
}

// Gets source endpoint for the whole driver (or persqueue client)
// and endpoint that was given us by the cluster discovery service
// and gives endpoint for the current LB cluster.
// For examples see tests.
std::string ApplyClusterEndpoint(std::string_view driverEndpoint, const std::string& clusterDiscoveryEndpoint);

// Factory for IStreamRequestReadWriteProcessor
// It is created in order to separate grpc transport logic from
// the logic of session.
// So there is grpc factory implementation to use in SDK
// and another one to use in tests for testing only session logic
// without transport stuff.
template <class TRequest, class TResponse>
struct ISessionConnectionProcessorFactory {
    using IProcessor = NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>;
    using TConnectedCallback = std::function<void(TPlainStatus&&, typename IProcessor::TPtr&&)>;
    using TConnectTimeoutCallback = std::function<void(bool ok)>;

    virtual ~ISessionConnectionProcessorFactory() = default;

    // Creates processor
    virtual void CreateProcessor(
        // Params for connect.
        TConnectedCallback callback,
        const TRpcRequestSettings& requestSettings,
        NYdbGrpc::IQueueClientContextPtr connectContext,
        // Params for timeout and its cancellation.
        TDuration connectTimeout,
        NYdbGrpc::IQueueClientContextPtr connectTimeoutContext,
        TConnectTimeoutCallback connectTimeoutCallback,
        // Params for delay before reconnect and its cancellation.
        TDuration connectDelay = TDuration::Zero(),
        NYdbGrpc::IQueueClientContextPtr connectDelayOperationContext = nullptr) = 0;
};

template <class TService, class TRequest, class TResponse>
class TSessionConnectionProcessorFactory : public ISessionConnectionProcessorFactory<TRequest, TResponse>,
                                           public std::enable_shared_from_this<TSessionConnectionProcessorFactory<TService, TRequest, TResponse>>
{
public:
    using TConnectedCallback = typename ISessionConnectionProcessorFactory<TRequest, TResponse>::TConnectedCallback;
    using TConnectTimeoutCallback = typename ISessionConnectionProcessorFactory<TRequest, TResponse>::TConnectTimeoutCallback;
    TSessionConnectionProcessorFactory(
        TGRpcConnectionsImpl::TStreamRpc<TService, TRequest, TResponse, NYdbGrpc::TStreamRequestReadWriteProcessor> rpc,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbState
    )
        : Rpc(rpc)
        , Connections(std::move(connections))
        , DbDriverState(dbState)
    {
    }

    void CreateProcessor(
        TConnectedCallback callback,
        const TRpcRequestSettings& requestSettings,
        NYdbGrpc::IQueueClientContextPtr connectContext,
        TDuration connectTimeout,
        NYdbGrpc::IQueueClientContextPtr connectTimeoutContext,
        TConnectTimeoutCallback connectTimeoutCallback,
        TDuration connectDelay,
        NYdbGrpc::IQueueClientContextPtr connectDelayOperationContext) override
    {
        Y_ASSERT(connectContext);
        Y_ASSERT(connectTimeoutContext);
        Y_ASSERT((connectDelay == TDuration::Zero()) == !connectDelayOperationContext);
        if (connectDelay == TDuration::Zero()) {
            Connect(std::move(callback),
                    requestSettings,
                    std::move(connectContext),
                    connectTimeout,
                    std::move(connectTimeoutContext),
                    std::move(connectTimeoutCallback));
        } else {
            auto connect = [
                weakThis = this->weak_from_this(),
                callback = std::move(callback),
                requestSettings,
                connectContext = std::move(connectContext),
                connectTimeout,
                connectTimeoutContext = std::move(connectTimeoutContext),
                connectTimeoutCallback = std::move(connectTimeoutCallback)
            ] (bool ok)
            {
                if (!ok) {
                    return;
                }

                if (auto sharedThis = weakThis.lock()) {
                    sharedThis->Connect(
                        std::move(callback),
                        requestSettings,
                        std::move(connectContext),
                        connectTimeout,
                        std::move(connectTimeoutContext),
                        std::move(connectTimeoutCallback)
                    );
                }
            };

            Connections->ScheduleCallback(
                connectDelay,
                std::move(connect),
                std::move(connectDelayOperationContext)
            );
        }
    }

private:
    void Connect(
        TConnectedCallback callback,
        const TRpcRequestSettings& requestSettings,
        NYdbGrpc::IQueueClientContextPtr connectContext,
        TDuration connectTimeout,
        NYdbGrpc::IQueueClientContextPtr connectTimeoutContext,
        TConnectTimeoutCallback connectTimeoutCallback)
    {
        Connections->StartBidirectionalStream<TService, TRequest, TResponse>(
            std::move(callback),
            Rpc,
            DbDriverState,
            requestSettings,
            std::move(connectContext)
        );

        Connections->ScheduleCallback(
            connectTimeout,
            std::move(connectTimeoutCallback),
            std::move(connectTimeoutContext)
        );
    }

private:
    TGRpcConnectionsImpl::TStreamRpc<TService, TRequest, TResponse, NYdbGrpc::TStreamRequestReadWriteProcessor> Rpc;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TDbDriverStatePtr DbDriverState;
};

template <class TService, class TRequest, class TResponse>
std::shared_ptr<ISessionConnectionProcessorFactory<TRequest, TResponse>>
    CreateConnectionProcessorFactory(
        TGRpcConnectionsImpl::TStreamRpc<TService, TRequest, TResponse, NYdbGrpc::TStreamRequestReadWriteProcessor> rpc,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbState
    )
{
    return std::make_shared<TSessionConnectionProcessorFactory<TService, TRequest, TResponse>>(rpc, std::move(connections), std::move(dbState));
}



template <class TEvent_>
struct TBaseEventInfo {
    using TEvent = TEvent_;

    TEvent Event;

    TEvent& GetEvent() {
        return Event;
    }

    void OnUserRetrievedEvent() {
    }

    template <class T>
    TBaseEventInfo(T&& event)
        : Event(std::forward<T>(event))
    {}
};


class ISignalable {
public:
    ISignalable() = default;
    virtual ~ISignalable() {}
    virtual void Signal() = 0;
};

// Waiter on queue.
// Future or GetEvent call
class TWaiter {
public:
    TWaiter() = default;

    TWaiter(const TWaiter&) = delete;
    TWaiter& operator=(const TWaiter&) = delete;
    TWaiter(TWaiter&&) = default;
    TWaiter& operator=(TWaiter&&) = default;

    TWaiter(NThreading::TPromise<void>&& promise, ISignalable* self)
        : Promise(promise)
        , Future(promise.Initialized() ? Promise.GetFuture() : NThreading::TFuture<void>())
        , Self(self)
    {
    }

    void Signal() {
        if (Self) {
            Self->Signal();
        }
        if (Promise.Initialized() && !Promise.HasValue()) {
            Promise.SetValue();
        }
    }

    bool Valid() const {
        if (!Future.Initialized()) return false;
        return !Promise.Initialized() || Promise.GetFuture().StateId() == Future.StateId();
    }

    NThreading::TPromise<void> ExtractPromise() {
        NThreading::TPromise<void> promise;
        Y_ABORT_UNLESS(!promise.Initialized());
        std::swap(Promise, promise);
        return promise;
    }

    NThreading::TFuture<void> GetFuture() {
        Y_ABORT_UNLESS(Future.Initialized());
        return Future;
    }

private:
    NThreading::TPromise<void> Promise;
    NThreading::TFuture<void> Future;
    ISignalable* Self = nullptr;
};



// Class that is responsible for:
// - events queue;
// - signalling futures that wait for events;
// - packing events for waiters;
// - waking up waiters.
// Thread safe.
template <class TSettings_, class TEvent_, class TClosedEvent_, class TExecutor_, class TEventInfo_ = TBaseEventInfo<TEvent_>>
class TBaseSessionEventsQueue : public ISignalable {
protected:
    using TSelf = TBaseSessionEventsQueue<TSettings_, TEvent_, TClosedEvent_, TExecutor_, TEventInfo_>;
    using TSettings = TSettings_;
    using TEvent = TEvent_;
    using TEventInfo = TEventInfo_;
    using TClosedEvent = TClosedEvent_;
    using TExecutor = TExecutor_;

    // Template for visitor implementation.
    struct TBaseHandlersVisitor {
        TBaseHandlersVisitor(const TSettings& settings, TEvent& event)
            : Settings(settings)
            , Event(event)
        {}

        template <class TEventType, class TFunc, class TCommonFunc>
        bool PushHandler(TEvent&& event, const TFunc& specific, const TCommonFunc& common) {
            if (specific) {
                PushSpecificHandler<TEventType>(std::move(event), specific);
                return true;
            }
            if (common) {
                PushCommonHandler(std::move(event), common);
                return true;
            }
            return false;
        }

        template <class TEventType, class TFunc>
        void PushSpecificHandler(TEvent&& event, const TFunc& f) {
            Post(Settings.EventHandlers_.HandlersExecutor_,
                 [func = f, event = std::move(event)]() mutable {
                     func(std::get<TEventType>(event));
                 });
        }

        template <class TFunc>
        void PushCommonHandler(TEvent&& event, const TFunc& f) {
            Post(Settings.EventHandlers_.HandlersExecutor_,
                 [func = f, event = std::move(event)]() mutable { func(event); });
        }

        virtual void Post(const typename TExecutor::TPtr& executor, typename TExecutor::TFunction&& f) {
            executor->Post(std::move(f));
        }

        const TSettings& Settings;
        TEvent& Event;
    };


public:
    TBaseSessionEventsQueue(const TSettings& settings)
        : Settings(settings)
        , Waiter(NThreading::NewPromise<void>(), this)
    {}

    virtual ~TBaseSessionEventsQueue() = default;


    void Signal() override {
        CondVar.notify_one();
    }

protected:
    virtual bool HasEventsImpl() const {  // Assumes that we're under lock.
        return !Events.empty() || CloseEvent;
    }

    TWaiter PopWaiterImpl() { // Assumes that we're under lock.
        TWaiter waiter(Waiter.ExtractPromise(), this);
        WaiterWillBeSignaled = true;
        return std::move(waiter);
    }

    void WaitEventsImpl() { // Assumes that we're under lock. Posteffect: HasEventsImpl() is true.
        while (!HasEventsImpl()) {
            std::unique_lock<std::mutex> lk(Mutex, std::adopt_lock);
            CondVar.wait(lk);
            lk.release();
        }
    }

    void RenewWaiterImpl() {
        if (Events.empty() && WaiterWillBeSignaled) {
            Waiter = TWaiter(NThreading::NewPromise<void>(), this);
            WaiterWillBeSignaled = false;
        }
    }

    virtual void SelfCheck() {
    }

public:
    NThreading::TFuture<void> WaitEvent() {
        bool needSelfCheck = false;
        NThreading::TFuture<void> res;
        {
            std::lock_guard<std::mutex> guard (Mutex);
            if (HasEventsImpl()) {
                return NThreading::MakeFuture(); // Signalled
            } else {
                if (const auto now = TInstant::Now(); now - LastSelfCheckAt > TDuration::Seconds(10)) {
                    LastSelfCheckAt = now;
                    needSelfCheck = true;
                }

                Y_ABORT_UNLESS(Waiter.Valid());
                res = Waiter.GetFuture();
            }
        }
        if (needSelfCheck) {
            SelfCheck();
        }
        return res;
    }

    bool IsClosed() {
        return Closed;
    }

protected:
    const TSettings& Settings;
    TWaiter Waiter;
    bool WaiterWillBeSignaled = false;
    std::queue<TEventInfo> Events;
    std::condition_variable CondVar;
    std::mutex Mutex;
    std::optional<TClosedEvent> CloseEvent;
    std::atomic<bool> Closed = false;

private:
    TInstant LastSelfCheckAt = TInstant::Now();
};

} // namespace NYdb::NTopic
