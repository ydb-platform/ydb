#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/include/errors.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_events.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <util/generic/queue.h>
#include <util/system/condvar.h>
#include <util/thread/pool.h>

#include <queue>

namespace NYdb::NTopic {

ERetryErrorClass GetRetryErrorClass(EStatus status);
ERetryErrorClass GetRetryErrorClassV2(EStatus status);

void Cancel(NYdbGrpc::IQueueClientContextPtr& context);

NYql::TIssues MakeIssueWithSubIssues(const TString& description, const NYql::TIssues& subissues);

TString IssuesSingleLineString(const NYql::TIssues& issues);

template <typename TEvent>
size_t CalcDataSize(const typename TEvent::TEvent& event) {
    constexpr bool UseMigrationProtocol = !std::is_same_v<TEvent, TReadSessionEvent>;

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
    NYql::TIssues issues;
    NYql::IssuesFromMessage(serverMessage.issues(), issues);
    return TPlainStatus(static_cast<EStatus>(serverMessage.status()), std::move(issues));
}

// Gets source endpoint for the whole driver (or persqueue client)
// and endpoint that was given us by the cluster discovery service
// and gives endpoint for the current LB cluster.
// For examples see tests.
TString ApplyClusterEndpoint(TStringBuf driverEndpoint, const TString& clusterDiscoveryEndpoint);

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
        CondVar.Signal();
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
            CondVar.WaitI(Mutex);
        }
    }

    void RenewWaiterImpl() {
        if (Events.empty() && WaiterWillBeSignaled) {
            Waiter = TWaiter(NThreading::NewPromise<void>(), this);
            WaiterWillBeSignaled = false;
        }
    }

public:
    NThreading::TFuture<void> WaitEvent() {
        with_lock (Mutex) {
            if (HasEventsImpl()) {
                return NThreading::MakeFuture(); // Signalled
            } else {
                Y_ABORT_UNLESS(Waiter.Valid());
                auto res = Waiter.GetFuture();
                return res;
            }
        }
    }

    bool IsClosed() {
        return Closed;
    }

protected:
    const TSettings& Settings;
    TWaiter Waiter;
    bool WaiterWillBeSignaled = false;
    std::queue<TEventInfo> Events;
    TCondVar CondVar;
    TMutex Mutex;
    TMaybe<TClosedEvent> CloseEvent;
    std::atomic<bool> Closed = false;
};

}  // namespace NYdb::NTopic
