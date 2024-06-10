#pragma once

#include "grpc_common.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/thread/factory.h>
#include <util/string/builder.h>
#include <grpc++/grpc++.h>
#include <grpc++/support/async_stream.h>
#include <grpc++/support/async_unary_call.h>

#include <deque>
#include <typeindex>
#include <typeinfo>
#include <variant>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>

/*
 * This file contains low level logic for grpc
 * This file should not be used in high level code without special reason
 */
namespace NYdbGrpc {

const size_t DEFAULT_NUM_THREADS = 2;

////////////////////////////////////////////////////////////////////////////////

void EnableGRpcTracing();

////////////////////////////////////////////////////////////////////////////////

struct TTcpKeepAliveSettings {
    bool Enabled;
    size_t Idle;
    size_t Count;
    size_t Interval;
};

////////////////////////////////////////////////////////////////////////////////

// Common interface used to execute action from grpc cq routine
class IQueueClientEvent {
public:
    virtual ~IQueueClientEvent() = default;

    //! Execute an action defined by implementation
    virtual bool Execute(bool ok) = 0;

    //! Finish and destroy event
    virtual void Destroy() = 0;
};

// Implementation of IQueueClientEvent that reduces allocations
template<class TSelf>
class TQueueClientFixedEvent : private IQueueClientEvent {
    using TCallback = void (TSelf::*)(bool);

public:
    TQueueClientFixedEvent(TSelf* self, TCallback callback)
        : Self(self)
        , Callback(callback)
    { }

    IQueueClientEvent* Prepare() {
        Self->Ref();
        return this;
    }

private:
    bool Execute(bool ok) override {
        ((*Self).*Callback)(ok);
        return false;
    }

    void Destroy() override {
        Self->UnRef();
    }

private:
    TSelf* const Self;
    TCallback const Callback;
};

class IQueueClientContext;
using IQueueClientContextPtr = std::shared_ptr<IQueueClientContext>;

// Provider of IQueueClientContext instances
class IQueueClientContextProvider {
public:
    virtual ~IQueueClientContextProvider() = default;

    virtual IQueueClientContextPtr CreateContext() = 0;
};

// Activity context for a low-level client
class IQueueClientContext : public IQueueClientContextProvider {
public:
    virtual ~IQueueClientContext() = default;

    //! Returns CompletionQueue associated with the client
    virtual grpc::CompletionQueue* CompletionQueue() = 0;

    //! Returns true if context has been cancelled
    virtual bool IsCancelled() const = 0;

    //! Tries to cancel context, calling all registered callbacks
    virtual bool Cancel() = 0;

    //! Subscribes callback to cancellation
    //
    // Note there's no way to unsubscribe, if subscription is temporary
    // make sure you create a new context with CreateContext and release
    // it as soon as it's no longer needed.
    virtual void SubscribeCancel(std::function<void()> callback) = 0;

    //! Subscribes callback to cancellation
    //
    // This alias is for compatibility with older code.
    void SubscribeStop(std::function<void()> callback) {
        SubscribeCancel(std::move(callback));
    }
};

// Represents grpc status and error message string
struct TGrpcStatus {
    TString Msg;
    TString Details;
    int GRpcStatusCode;
    bool InternalError;
    std::multimap<TString, TString> ServerTrailingMetadata;

    TGrpcStatus()
        : GRpcStatusCode(grpc::StatusCode::OK)
        , InternalError(false)
    { }

    TGrpcStatus(TString msg, int statusCode, bool internalError)
        : Msg(std::move(msg))
        , GRpcStatusCode(statusCode)
        , InternalError(internalError)
    { }

    TGrpcStatus(grpc::StatusCode status, TString msg, TString details = {})
        : Msg(std::move(msg))
        , Details(std::move(details))
        , GRpcStatusCode(status)
        , InternalError(false)
    { }

    TGrpcStatus(const grpc::Status& status)
        : TGrpcStatus(status.error_code(), TString(status.error_message()), TString(status.error_details()))
    { }

    TGrpcStatus& operator=(const grpc::Status& status) {
        Msg = TString(status.error_message());
        Details = TString(status.error_details());
        GRpcStatusCode = status.error_code();
        InternalError = false;
        return *this;
    }

    static TGrpcStatus Internal(TString msg) {
        return { std::move(msg), -1, true };
    }

    bool Ok() const {
        return !InternalError && GRpcStatusCode == grpc::StatusCode::OK;
    }

    TStringBuilder ToDebugString() const {
        TStringBuilder ret;
        ret << "gRpcStatusCode: " << GRpcStatusCode;
        if(!Ok())
            ret << ", Msg: " << Msg << ", Details: " << Details << ", InternalError: " << InternalError;
        return ret;
    }
};

bool inline IsGRpcStatusGood(const TGrpcStatus& status) {
    return status.Ok();
}

// Response callback type - this callback will be called when request is finished
//  (or after getting each chunk in case of streaming mode)
template<typename TResponse>
using TResponseCallback = std::function<void (TGrpcStatus&&, TResponse&&)>;

template<typename TResponse>
using TAdvancedResponseCallback = std::function<void (const grpc::ClientContext&, TGrpcStatus&&, TResponse&&)>;

// Call associated metadata
struct TCallMeta {
    std::shared_ptr<grpc::CallCredentials> CallCredentials;
    std::vector<std::pair<TString, TString>> Aux;
    std::variant<TDuration, TInstant> Timeout; // timeout as duration from now or time point in future
};

class TGRpcRequestProcessorCommon {
protected:
    void ApplyMeta(const TCallMeta& meta) {
        for (const auto& rec : meta.Aux) {
            Context.AddMetadata(rec.first, rec.second);
        }
        if (meta.CallCredentials) {
            Context.set_credentials(meta.CallCredentials);
        }
        if (const TDuration* timeout = std::get_if<TDuration>(&meta.Timeout)) {
            if (*timeout) {
                auto deadline = gpr_time_add(
                        gpr_now(GPR_CLOCK_MONOTONIC),
                        gpr_time_from_micros(timeout->MicroSeconds(), GPR_TIMESPAN));
                Context.set_deadline(deadline);
            }
        } else if (const TInstant* deadline = std::get_if<TInstant>(&meta.Timeout)) {
            if (*deadline) {
                Context.set_deadline(gpr_time_from_micros(deadline->MicroSeconds(), GPR_CLOCK_MONOTONIC));
            }
        }
    }

    void GetInitialMetadata(std::unordered_multimap<TString, TString>* metadata) {
        for (const auto& [key, value] : Context.GetServerInitialMetadata()) {
            metadata->emplace(
                TString(key.begin(), key.end()),
                TString(value.begin(), value.end())
            );
        }
    }

    grpc::Status Status;
    grpc::ClientContext Context;
    std::shared_ptr<IQueueClientContext> LocalContext;
};

template<typename TStub, typename TRequest, typename TResponse>
class TSimpleRequestProcessor
    : public TThrRefBase
    , public IQueueClientEvent
    , public TGRpcRequestProcessorCommon {
    using TAsyncReaderPtr = std::unique_ptr<grpc::ClientAsyncResponseReader<TResponse>>;
    template<typename> friend class TServiceConnection;
public:
    using TPtr = TIntrusivePtr<TSimpleRequestProcessor>;
    using TAsyncRequest = TAsyncReaderPtr (TStub::*)(grpc::ClientContext*, const TRequest&, grpc::CompletionQueue*);

    explicit TSimpleRequestProcessor(TResponseCallback<TResponse>&& callback)
        : Callback_(std::move(callback))
    { }

    ~TSimpleRequestProcessor() {
        if (!Replied_ && Callback_) {
            Callback_(TGrpcStatus::Internal("request left unhandled"), std::move(Reply_));
            Callback_ = nullptr; // free resources as early as possible
        }
    }

    bool Execute(bool ok) override {
        {
            std::unique_lock<std::mutex> guard(Mutex_);
            LocalContext.reset();
        }
        TGrpcStatus status;
        if (ok) {
            status = Status;
        } else {
            status = TGrpcStatus::Internal("Unexpected error");
        }
        Replied_ = true;
        Callback_(std::move(status), std::move(Reply_));
        Callback_ = nullptr; // free resources as early as possible
        return false;
    }

    void Destroy() override {
        UnRef();
    }

private:
    IQueueClientEvent* FinishedEvent() {
        Ref();
        return this;
    }

    void Start(TStub& stub, TAsyncRequest asyncRequest, const TRequest& request, IQueueClientContextProvider* provider) {
        auto context = provider->CreateContext();
        if (!context) {
            Replied_ = true;
            Callback_(TGrpcStatus(grpc::StatusCode::CANCELLED, "Client is shutting down"), std::move(Reply_));
            Callback_ = nullptr;
            return;
        }
        {
            std::unique_lock<std::mutex> guard(Mutex_);
            LocalContext = context;
            Reader_ = (stub.*asyncRequest)(&Context, request, context->CompletionQueue());
            Reader_->Finish(&Reply_, &Status, FinishedEvent());
        }
        context->SubscribeStop([self = TPtr(this)] {
            self->Stop();
        });
    }

    void Stop() {
        Context.TryCancel();
    }

    TResponseCallback<TResponse> Callback_;
    TResponse Reply_;
    std::mutex Mutex_;
    TAsyncReaderPtr Reader_;

    bool Replied_ = false;
};

template<typename TStub, typename TRequest, typename TResponse>
class TAdvancedRequestProcessor
    : public TThrRefBase
    , public IQueueClientEvent
    , public TGRpcRequestProcessorCommon {
    using TAsyncReaderPtr = std::unique_ptr<grpc::ClientAsyncResponseReader<TResponse>>;
    template<typename> friend class TServiceConnection;
public:
    using TPtr = TIntrusivePtr<TAdvancedRequestProcessor>;
    using TAsyncRequest = TAsyncReaderPtr (TStub::*)(grpc::ClientContext*, const TRequest&, grpc::CompletionQueue*);

    explicit TAdvancedRequestProcessor(TAdvancedResponseCallback<TResponse>&& callback)
        : Callback_(std::move(callback))
    { }

    ~TAdvancedRequestProcessor() {
        if (!Replied_ && Callback_) {
            Callback_(Context, TGrpcStatus::Internal("request left unhandled"), std::move(Reply_));
            Callback_ = nullptr; // free resources as early as possible
        }
    }

    bool Execute(bool ok) override {
        {
            std::unique_lock<std::mutex> guard(Mutex_);
            LocalContext.reset();
        }
        TGrpcStatus status;
        if (ok) {
            status = Status;
        } else {
            status = TGrpcStatus::Internal("Unexpected error");
        }
        Replied_ = true;
        Callback_(Context, std::move(status), std::move(Reply_));
        Callback_ = nullptr; // free resources as early as possible
        return false;
    }

    void Destroy() override {
        UnRef();
    }

private:
    IQueueClientEvent* FinishedEvent() {
        Ref();
        return this;
    }

    void Start(TStub& stub, TAsyncRequest asyncRequest, const TRequest& request, IQueueClientContextProvider* provider) {
        auto context = provider->CreateContext();
        if (!context) {
            Replied_ = true;
            Callback_(Context, TGrpcStatus(grpc::StatusCode::CANCELLED, "Client is shutting down"), std::move(Reply_));
            Callback_ = nullptr;
            return;
        }
        {
            std::unique_lock<std::mutex> guard(Mutex_);
            LocalContext = context;
            Reader_ = (stub.*asyncRequest)(&Context, request, context->CompletionQueue());
            Reader_->Finish(&Reply_, &Status, FinishedEvent());
        }
        context->SubscribeStop([self = TPtr(this)] {
            self->Stop();
        });
    }

    void Stop() {
        Context.TryCancel();
    }

    TAdvancedResponseCallback<TResponse> Callback_;
    TResponse Reply_;
    std::mutex Mutex_;
    TAsyncReaderPtr Reader_;

    bool Replied_ = false;
};

class IStreamRequestCtrl : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IStreamRequestCtrl>;

    /**
     * Asynchronously cancel the request
     */
    virtual void Cancel() = 0;
};

template<class TResponse>
class IStreamRequestReadProcessor : public IStreamRequestCtrl {
public:
    using TPtr = TIntrusivePtr<IStreamRequestReadProcessor>;
    using TReadCallback = std::function<void(TGrpcStatus&&)>;

    /**
     * Scheduled initial server metadata read from the stream
     */
    virtual void ReadInitialMetadata(std::unordered_multimap<TString, TString>* metadata, TReadCallback callback) = 0;

    /**
     * Scheduled response read from the stream
     * Callback will be called with the status if it failed
     * Only one Read or Finish call may be active at a time
     */
    virtual void Read(TResponse* response, TReadCallback callback) = 0;

    /**
     * Stop reading and gracefully finish the stream
     * Only one Read or Finish call may be active at a time
     */
    virtual void Finish(TReadCallback callback) = 0;

    /**
     * Additional callback to be called when stream has finished
     */
    virtual void AddFinishedCallback(TReadCallback callback) = 0;
};

template<class TRequest, class TResponse>
class IStreamRequestReadWriteProcessor : public IStreamRequestReadProcessor<TResponse> {
public:
    using TPtr = TIntrusivePtr<IStreamRequestReadWriteProcessor>;
    using TWriteCallback = std::function<void(TGrpcStatus&&)>;

    /**
     * Scheduled request write to the stream
     */
    virtual void Write(TRequest&& request, TWriteCallback callback = { }) = 0;
};

class TGRpcKeepAliveSocketMutator;

// Class to hold stubs allocated on channel.
// It is poor documented part of grpc. See KIKIMR-6109 and comment to this commit

// Stub holds shared_ptr<ChannelInterface>, so we can destroy this holder even if
// request processor using stub
class TStubsHolder : public TNonCopyable {
    using TypeInfoRef = std::reference_wrapper<const std::type_info>;

    struct THasher {
        std::size_t operator()(TypeInfoRef code) const {
            return code.get().hash_code();
        }
    };

    struct TEqualTo {
        bool operator()(TypeInfoRef lhs, TypeInfoRef rhs) const {
            return lhs.get() == rhs.get();
        }
    };
public:
    TStubsHolder(std::shared_ptr<grpc::ChannelInterface> channel)
        : ChannelInterface_(channel)
    {}

    // Returns true if channel can't be used to perform request now
    bool IsChannelBroken() const {
        auto state = ChannelInterface_->GetState(false);
        return state == GRPC_CHANNEL_SHUTDOWN ||
            state == GRPC_CHANNEL_TRANSIENT_FAILURE;
    }

    template<typename TStub>
    std::shared_ptr<TStub> GetOrCreateStub() {
        const auto& stubId = typeid(TStub);
        {
            std::shared_lock readGuard(RWMutex_);
            const auto it = Stubs_.find(stubId);
            if (it != Stubs_.end()) {
                return std::static_pointer_cast<TStub>(it->second);
            }
        }
        {
            std::unique_lock writeGuard(RWMutex_);
            auto it = Stubs_.emplace(stubId, nullptr);
            if (!it.second) {
                return std::static_pointer_cast<TStub>(it.first->second);
            } else {
                it.first->second = std::make_shared<TStub>(ChannelInterface_);
                return std::static_pointer_cast<TStub>(it.first->second);
            }
        }
    }

    const TInstant& GetLastUseTime() const {
        return LastUsed_;
    }

    void SetLastUseTime(const TInstant& time) {
        LastUsed_ = time;
    }
private:
    TInstant LastUsed_ = Now();
    std::shared_mutex RWMutex_;
    std::unordered_map<TypeInfoRef, std::shared_ptr<void>, THasher, TEqualTo> Stubs_;
    std::shared_ptr<grpc::ChannelInterface> ChannelInterface_;
};

class TChannelPool {
public:
    TChannelPool(const TTcpKeepAliveSettings& tcpKeepAliveSettings, const TDuration& expireTime = TDuration::Minutes(6));
    //Allows to CreateStub from TStubsHolder under lock
    //The callback will be called just during GetStubsHolderLocked call
    void GetStubsHolderLocked(const TString& channelId, const TGRpcClientConfig& config, std::function<void(TStubsHolder&)> cb);
    void DeleteChannel(const TString& channelId);
    void DeleteExpiredStubsHolders();
private:
    std::shared_mutex RWMutex_;
    std::unordered_map<TString, TStubsHolder> Pool_;
    std::multimap<TInstant, TString> LastUsedQueue_;
    TTcpKeepAliveSettings TcpKeepAliveSettings_;
    TDuration ExpireTime_;
    TDuration UpdateReUseTime_;
    void EraseFromQueueByTime(const TInstant& lastUseTime, const TString& channelId);
};

template<class TResponse>
using TStreamReaderCallback = std::function<void(TGrpcStatus&&, typename IStreamRequestReadProcessor<TResponse>::TPtr)>;

template<typename TStub, typename TRequest, typename TResponse>
class TStreamRequestReadProcessor
    : public IStreamRequestReadProcessor<TResponse>
    , public TGRpcRequestProcessorCommon {
    template<typename> friend class TServiceConnection;
public:
    using TSelf = TStreamRequestReadProcessor;
    using TAsyncReaderPtr = std::unique_ptr<grpc::ClientAsyncReader<TResponse>>;
    using TAsyncRequest = TAsyncReaderPtr (TStub::*)(grpc::ClientContext*, const TRequest&, grpc::CompletionQueue*, void*);
    using TReaderCallback = TStreamReaderCallback<TResponse>;
    using TPtr = TIntrusivePtr<TSelf>;
    using TBase = IStreamRequestReadProcessor<TResponse>;
    using TReadCallback = typename TBase::TReadCallback;

    explicit TStreamRequestReadProcessor(TReaderCallback&& callback)
        : Callback(std::move(callback))
    {
        Y_ABORT_UNLESS(Callback, "Missing connected callback");
    }

    void Cancel() override {
        Context.TryCancel();

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Cancelled = true;
            if (Started && !ReadFinished) {
                if (!ReadActive) {
                    ReadFinished = true;
                }
                if (ReadFinished) {
                    Stream->Finish(&Status, OnFinishedTag.Prepare());
                }
            }
        }
    }

    void ReadInitialMetadata(std::unordered_multimap<TString, TString>* metadata, TReadCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(!ReadActive, "Multiple Read/Finish calls detected");
            if (!Finished && !HasInitialMetadata) {
                ReadActive = true;
                ReadCallback = std::move(callback);
                InitialMetadata = metadata;
                if (!ReadFinished) {
                    Stream->ReadInitialMetadata(OnReadDoneTag.Prepare());
                }
                return;
            }
            if (!HasInitialMetadata) {
                if (FinishedOk) {
                    status = Status;
                } else {
                    status = TGrpcStatus::Internal("Unexpected error");
                }
            } else {
                GetInitialMetadata(metadata);
            }
        }

        callback(std::move(status));
    }

    void Read(TResponse* message, TReadCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(!ReadActive, "Multiple Read/Finish calls detected");
            if (!Finished) {
                ReadActive = true;
                ReadCallback = std::move(callback);
                if (!ReadFinished) {
                    Stream->Read(message, OnReadDoneTag.Prepare());
                }
                return;
            }
            if (FinishedOk) {
                status = Status;
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }
        }

        if (status.Ok()) {
            status = TGrpcStatus(grpc::StatusCode::OUT_OF_RANGE, "Read EOF");
        }

        callback(std::move(status));
    }

    void Finish(TReadCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(!ReadActive, "Multiple Read/Finish calls detected");
            if (!Finished) {
                ReadActive = true;
                FinishCallback = std::move(callback);
                if (!ReadFinished) {
                    ReadFinished = true;
                }
                Stream->Finish(&Status, OnFinishedTag.Prepare());
                return;
            }
            if (FinishedOk) {
                status = Status;
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }
        }

        callback(std::move(status));
    }

    void AddFinishedCallback(TReadCallback callback) override {
        Y_ABORT_UNLESS(callback, "Unexpected empty callback");

        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            if (!Finished) {
                FinishedCallbacks.emplace_back().swap(callback);
                return;
            }

            if (FinishedOk) {
                status = Status;
            } else if (Cancelled) {
                status = TGrpcStatus(grpc::StatusCode::CANCELLED, "Stream cancelled");
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }
        }

        callback(std::move(status));
    }

private:
    void Start(TStub& stub, const TRequest& request, TAsyncRequest asyncRequest, IQueueClientContextProvider* provider) {
        auto context = provider->CreateContext();
        if (!context) {
            auto callback = std::move(Callback);
            TGrpcStatus status(grpc::StatusCode::CANCELLED, "Client is shutting down");
            callback(std::move(status), nullptr);
            return;
        }

        {
            std::unique_lock<std::mutex> guard(Mutex);
            LocalContext = context;
            Stream = (stub.*asyncRequest)(&Context, request, context->CompletionQueue(), OnStartDoneTag.Prepare());
        }

        context->SubscribeStop([self = TPtr(this)] {
            self->Cancel();
        });
    }

    void OnReadDone(bool ok) {
        TGrpcStatus status;
        TReadCallback callback;
        std::unordered_multimap<TString, TString>* initialMetadata = nullptr;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(ReadActive, "Unexpected Read done callback");
            Y_ABORT_UNLESS(!ReadFinished, "Unexpected ReadFinished flag");

            if (!ok || Cancelled) {
                ReadFinished = true;

                Stream->Finish(&Status, OnFinishedTag.Prepare());
                if (!ok) {
                    // Keep ReadActive=true, so callback is called
                    // after the call is finished with an error
                    return;
                }
            }

            callback = std::move(ReadCallback);
            ReadCallback = nullptr;
            ReadActive = false;
            initialMetadata = InitialMetadata;
            InitialMetadata = nullptr;
            HasInitialMetadata = true;
        }

        if (initialMetadata) {
            GetInitialMetadata(initialMetadata);
        }

        callback(std::move(status));
    }

    void OnStartDone(bool ok) {
        TReaderCallback callback;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Started = true;
            if (!ok || Cancelled) {
                ReadFinished = true;
                Stream->Finish(&Status, OnFinishedTag.Prepare());
                return;
            }
            callback = std::move(Callback);
            Callback = nullptr;
        }

        callback({ }, typename TBase::TPtr(this));
    }

    void OnFinished(bool ok) {
        TGrpcStatus status;
        std::vector<TReadCallback> finishedCallbacks;
        TReaderCallback startCallback;
        TReadCallback readCallback;
        TReadCallback finishCallback;

        {
            std::unique_lock<std::mutex> guard(Mutex);

            Finished = true;
            FinishedOk = ok;
            LocalContext.reset();

            if (ok) {
                status = Status;
            } else if (Cancelled) {
                status = TGrpcStatus(grpc::StatusCode::CANCELLED, "Stream cancelled");
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }

            finishedCallbacks.swap(FinishedCallbacks);

            if (Callback) {
                Y_ABORT_UNLESS(!ReadActive);
                startCallback = std::move(Callback);
                Callback = nullptr;
            } else if (ReadActive) {
                if (ReadCallback) {
                    readCallback = std::move(ReadCallback);
                    ReadCallback = nullptr;
                } else {
                    finishCallback = std::move(FinishCallback);
                    FinishCallback = nullptr;
                }
                ReadActive = false;
            }
        }

        for (auto& finishedCallback : finishedCallbacks) {
            auto statusCopy = status;
            finishedCallback(std::move(statusCopy));
        }

        if (startCallback) {
            if (status.Ok()) {
                status = TGrpcStatus(grpc::StatusCode::UNKNOWN, "Unknown stream failure");
            }
            startCallback(std::move(status), nullptr);
        } else if (readCallback) {
            if (status.Ok()) {
                status = TGrpcStatus(grpc::StatusCode::OUT_OF_RANGE, "Read EOF");
                for (const auto& [name, value] : Context.GetServerTrailingMetadata()) {
                    status.ServerTrailingMetadata.emplace(
                        TString(name.begin(), name.end()),
                        TString(value.begin(), value.end()));
                }
            }
            readCallback(std::move(status));
        } else if (finishCallback) {
            finishCallback(std::move(status));
        }
    }

    TReaderCallback Callback;
    TAsyncReaderPtr Stream;
    using TFixedEvent = TQueueClientFixedEvent<TSelf>;
    std::mutex Mutex;
    TFixedEvent OnReadDoneTag = { this, &TSelf::OnReadDone };
    TFixedEvent OnStartDoneTag = { this, &TSelf::OnStartDone };
    TFixedEvent OnFinishedTag = { this, &TSelf::OnFinished };

    TReadCallback ReadCallback;
    TReadCallback FinishCallback;
    std::vector<TReadCallback> FinishedCallbacks;
    std::unordered_multimap<TString, TString>* InitialMetadata = nullptr;
    bool Started = false;
    bool HasInitialMetadata = false;
    bool ReadActive = false;
    bool ReadFinished = false;
    bool Finished = false;
    bool Cancelled = false;
    bool FinishedOk = false;
};

template<class TRequest, class TResponse>
using TStreamConnectedCallback = std::function<void(TGrpcStatus&&, typename IStreamRequestReadWriteProcessor<TRequest, TResponse>::TPtr)>;

template<class TStub, class TRequest, class TResponse>
class TStreamRequestReadWriteProcessor
    : public IStreamRequestReadWriteProcessor<TRequest, TResponse>
    , public TGRpcRequestProcessorCommon {
public:
    using TSelf = TStreamRequestReadWriteProcessor;
    using TBase = IStreamRequestReadWriteProcessor<TRequest, TResponse>;
    using TPtr = TIntrusivePtr<TSelf>;
    using TConnectedCallback = TStreamConnectedCallback<TRequest, TResponse>;
    using TReadCallback = typename TBase::TReadCallback;
    using TWriteCallback = typename TBase::TWriteCallback;
    using TAsyncReaderWriterPtr = std::unique_ptr<grpc::ClientAsyncReaderWriter<TRequest, TResponse>>;
    using TAsyncRequest = TAsyncReaderWriterPtr (TStub::*)(grpc::ClientContext*, grpc::CompletionQueue*, void*);

    explicit TStreamRequestReadWriteProcessor(TConnectedCallback&& callback)
        : ConnectedCallback(std::move(callback))
    {
        Y_ABORT_UNLESS(ConnectedCallback, "Missing connected callback");
    }

    void Cancel() override {
        Context.TryCancel();

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Cancelled = true;
            if (Started && !(ReadFinished && WriteFinished)) {
                if (!ReadActive) {
                    ReadFinished = true;
                }
                if (!WriteActive) {
                    WriteFinished = true;
                }
                if (ReadFinished && WriteFinished) {
                    Stream->Finish(&Status, OnFinishedTag.Prepare());
                }
            }
        }
    }

    void Write(TRequest&& request, TWriteCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            if (Cancelled || ReadFinished || WriteFinished) {
                status = TGrpcStatus(grpc::StatusCode::CANCELLED, "Write request dropped");
            } else if (WriteActive) {
                auto& item = WriteQueue.emplace_back();
                item.Callback.swap(callback);
                item.Request.Swap(&request);
            } else {
                WriteActive = true;
                WriteCallback.swap(callback);
                Stream->Write(request, OnWriteDoneTag.Prepare());
            }
        }

        if (!status.Ok() && callback) {
            callback(std::move(status));
        }
    }

    void ReadInitialMetadata(std::unordered_multimap<TString, TString>* metadata, TReadCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(!ReadActive, "Multiple Read/Finish calls detected");
            if (!Finished && !HasInitialMetadata) {
                ReadActive = true;
                ReadCallback = std::move(callback);
                InitialMetadata = metadata;
                if (!ReadFinished) {
                    Stream->ReadInitialMetadata(OnReadDoneTag.Prepare());
                }
                return;
            }
            if (!HasInitialMetadata) {
                if (FinishedOk) {
                    status = Status;
                } else {
                    status = TGrpcStatus::Internal("Unexpected error");
                }
            } else {
                GetInitialMetadata(metadata);
            }
        }

        callback(std::move(status));
    }

    void Read(TResponse* message, TReadCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(!ReadActive, "Multiple Read/Finish calls detected");
            if (!Finished) {
                ReadActive = true;
                ReadCallback = std::move(callback);
                if (!ReadFinished) {
                    Stream->Read(message, OnReadDoneTag.Prepare());
                }
                return;
            }
            if (FinishedOk) {
                status = Status;
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }
        }

        if (status.Ok()) {
            status = TGrpcStatus(grpc::StatusCode::OUT_OF_RANGE, "Read EOF");
        }

        callback(std::move(status));
    }

    void Finish(TReadCallback callback) override {
        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(!ReadActive, "Multiple Read/Finish calls detected");
            if (!Finished) {
                ReadActive = true;
                FinishCallback = std::move(callback);
                if (!ReadFinished) {
                    ReadFinished = true;
                    if (!WriteActive) {
                        WriteFinished = true;
                    }
                    if (WriteFinished) {
                        Stream->Finish(&Status, OnFinishedTag.Prepare());
                    }
                }
                return;
            }
            if (FinishedOk) {
                status = Status;
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }
        }

        callback(std::move(status));
    }

    void AddFinishedCallback(TReadCallback callback) override {
        Y_ABORT_UNLESS(callback, "Unexpected empty callback");

        TGrpcStatus status;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            if (!Finished) {
                FinishedCallbacks.emplace_back().swap(callback);
                return;
            }

            if (FinishedOk) {
                status = Status;
            } else if (Cancelled) {
                status = TGrpcStatus(grpc::StatusCode::CANCELLED, "Stream cancelled");
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }
        }

        callback(std::move(status));
    }

private:
    template<typename> friend class TServiceConnection;

    void Start(TStub& stub, TAsyncRequest asyncRequest, IQueueClientContextProvider* provider) {
        auto context = provider->CreateContext();
        if (!context) {
            auto callback = std::move(ConnectedCallback);
            TGrpcStatus status(grpc::StatusCode::CANCELLED, "Client is shutting down");
            callback(std::move(status), nullptr);
            return;
        }

        {
            std::unique_lock<std::mutex> guard(Mutex);
            LocalContext = context;
            Stream = (stub.*asyncRequest)(&Context, context->CompletionQueue(), OnConnectedTag.Prepare());
        }

        context->SubscribeStop([self = TPtr(this)] {
            self->Cancel();
        });
    }

private:
    void OnConnected(bool ok) {
        TConnectedCallback callback;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Started = true;
            if (!ok || Cancelled) {
                ReadFinished = true;
                WriteFinished = true;
                Stream->Finish(&Status, OnFinishedTag.Prepare());
                return;
            }

            callback = std::move(ConnectedCallback);
            ConnectedCallback = nullptr;
        }

        callback({ }, typename TBase::TPtr(this));
    }

    void OnReadDone(bool ok) {
        TGrpcStatus status;
        TReadCallback callback;
        std::unordered_multimap<TString, TString>* initialMetadata = nullptr;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(ReadActive, "Unexpected Read done callback");
            Y_ABORT_UNLESS(!ReadFinished, "Unexpected ReadFinished flag");

            if (!ok || Cancelled) {
                ReadFinished = true;
                if (!WriteActive) {
                    WriteFinished = true;
                }
                if (WriteFinished) {
                    Stream->Finish(&Status, OnFinishedTag.Prepare());
                }
                if (!ok) {
                    // Keep ReadActive=true, so callback is called
                    // after the call is finished with an error
                    return;
                }
            }

            callback = std::move(ReadCallback);
            ReadCallback = nullptr;
            ReadActive = false;
            initialMetadata = InitialMetadata;
            InitialMetadata = nullptr;
            HasInitialMetadata = true;
        }

        if (initialMetadata) {
            GetInitialMetadata(initialMetadata);
        }

        callback(std::move(status));
    }

    void OnWriteDone(bool ok) {
        TWriteCallback okCallback;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Y_ABORT_UNLESS(WriteActive, "Unexpected Write done callback");
            Y_ABORT_UNLESS(!WriteFinished, "Unexpected WriteFinished flag");

            if (ok) {
                okCallback.swap(WriteCallback);
            } else if (WriteCallback) {
                // Put callback back on the queue until OnFinished
                auto& item = WriteQueue.emplace_front();
                item.Callback.swap(WriteCallback);
            }

            if (!ok || Cancelled) {
                WriteActive = false;
                WriteFinished = true;
                if (ReadFinished) {
                    Stream->Finish(&Status, OnFinishedTag.Prepare());
                }
            } else if (!WriteQueue.empty()) {
                WriteCallback.swap(WriteQueue.front().Callback);
                Stream->Write(WriteQueue.front().Request, OnWriteDoneTag.Prepare());
                WriteQueue.pop_front();
            } else {
                WriteActive = false;
                if (ReadFinished) {
                    WriteFinished = true;
                    Stream->Finish(&Status, OnFinishedTag.Prepare());
                }
            }
        }

        if (okCallback) {
            okCallback(TGrpcStatus());
        }
    }

    void OnFinished(bool ok) {
        TGrpcStatus status;
        std::deque<TWriteItem> writesDropped;
        std::vector<TReadCallback> finishedCallbacks;
        TConnectedCallback connectedCallback;
        TReadCallback readCallback;
        TReadCallback finishCallback;

        {
            std::unique_lock<std::mutex> guard(Mutex);
            Finished = true;
            FinishedOk = ok;
            LocalContext.reset();

            if (ok) {
                status = Status;
            } else if (Cancelled) {
                status = TGrpcStatus(grpc::StatusCode::CANCELLED, "Stream cancelled");
            } else {
                status = TGrpcStatus::Internal("Unexpected error");
            }

            writesDropped.swap(WriteQueue);
            finishedCallbacks.swap(FinishedCallbacks);

            if (ConnectedCallback) {
                Y_ABORT_UNLESS(!ReadActive);
                connectedCallback = std::move(ConnectedCallback);
                ConnectedCallback = nullptr;
            } else if (ReadActive) {
                if (ReadCallback) {
                    readCallback = std::move(ReadCallback);
                    ReadCallback = nullptr;
                } else {
                    finishCallback = std::move(FinishCallback);
                    FinishCallback = nullptr;
                }
                ReadActive = false;
            }
        }

        for (auto& item : writesDropped) {
            if (item.Callback) {
                TGrpcStatus writeStatus = status;
                if (writeStatus.Ok()) {
                    writeStatus = TGrpcStatus(grpc::StatusCode::CANCELLED, "Write request dropped");
                }
                item.Callback(std::move(writeStatus));
            }
        }

        for (auto& finishedCallback : finishedCallbacks) {
            TGrpcStatus statusCopy = status;
            finishedCallback(std::move(statusCopy));
        }

        if (connectedCallback) {
            if (status.Ok()) {
                status = TGrpcStatus(grpc::StatusCode::UNKNOWN, "Unknown stream failure");
            }
            connectedCallback(std::move(status), nullptr);
        } else if (readCallback) {
            if (status.Ok()) {
                status = TGrpcStatus(grpc::StatusCode::OUT_OF_RANGE, "Read EOF");
                for (const auto& [name, value] : Context.GetServerTrailingMetadata()) {
                    status.ServerTrailingMetadata.emplace(
                        TString(name.begin(), name.end()),
                        TString(value.begin(), value.end()));
                }
            }
            readCallback(std::move(status));
        } else if (finishCallback) {
            finishCallback(std::move(status));
        }
    }

private:
    struct TWriteItem {
        TWriteCallback Callback;
        TRequest Request;
    };

private:
    using TFixedEvent = TQueueClientFixedEvent<TSelf>;

    TFixedEvent OnConnectedTag = { this, &TSelf::OnConnected };
    TFixedEvent OnReadDoneTag = { this, &TSelf::OnReadDone };
    TFixedEvent OnWriteDoneTag = { this, &TSelf::OnWriteDone };
    TFixedEvent OnFinishedTag = { this, &TSelf::OnFinished };

private:
    std::mutex Mutex;
    TAsyncReaderWriterPtr Stream;
    TConnectedCallback ConnectedCallback;
    TReadCallback ReadCallback;
    TReadCallback FinishCallback;
    std::vector<TReadCallback> FinishedCallbacks;
    std::deque<TWriteItem> WriteQueue;
    TWriteCallback WriteCallback;
    std::unordered_multimap<TString, TString>* InitialMetadata = nullptr;
    bool Started = false;
    bool HasInitialMetadata = false;
    bool ReadActive = false;
    bool ReadFinished = false;
    bool WriteActive = false;
    bool WriteFinished = false;
    bool Finished = false;
    bool Cancelled = false;
    bool FinishedOk = false;
};

class TGRpcClientLow;

template<typename TGRpcService>
class TServiceConnection {
    using TStub = typename TGRpcService::Stub;
    friend class TGRpcClientLow;

public:
    /*
     * Start simple request
     */
    template<typename TRequest, typename TResponse>
    void DoRequest(const TRequest& request,
                   TResponseCallback<TResponse> callback,
                   typename TSimpleRequestProcessor<TStub, TRequest, TResponse>::TAsyncRequest asyncRequest,
                   const TCallMeta& metas = { },
                   IQueueClientContextProvider* provider = nullptr)
    {
        auto processor = MakeIntrusive<TSimpleRequestProcessor<TStub, TRequest, TResponse>>(std::move(callback));
        processor->ApplyMeta(metas);
        processor->Start(*Stub_, asyncRequest, request, provider ? provider : Provider_);
    }

    /*
     * Start simple request
     */
    template<typename TRequest, typename TResponse>
    void DoAdvancedRequest(const TRequest& request,
                           TAdvancedResponseCallback<TResponse> callback,
                           typename TAdvancedRequestProcessor<TStub, TRequest, TResponse>::TAsyncRequest asyncRequest,
                           const TCallMeta& metas = { },
                           IQueueClientContextProvider* provider = nullptr)
    {
        auto processor = MakeIntrusive<TAdvancedRequestProcessor<TStub, TRequest, TResponse>>(std::move(callback));
        processor->ApplyMeta(metas);
        processor->Start(*Stub_, asyncRequest, request, provider ? provider : Provider_);
    }

    /*
     * Start bidirectional streamming
     */
    template<typename TRequest, typename TResponse>
    void DoStreamRequest(TStreamConnectedCallback<TRequest, TResponse> callback,
                         typename TStreamRequestReadWriteProcessor<TStub, TRequest, TResponse>::TAsyncRequest asyncRequest,
                         const TCallMeta& metas = { },
                         IQueueClientContextProvider* provider = nullptr)
    {
        auto processor = MakeIntrusive<TStreamRequestReadWriteProcessor<TStub, TRequest, TResponse>>(std::move(callback));
        processor->ApplyMeta(metas);
        processor->Start(*Stub_, std::move(asyncRequest), provider ? provider : Provider_);
    }

    /*
     * Start streaming response reading (one request, many responses)
     */
    template<typename TRequest, typename TResponse>
    void DoStreamRequest(const TRequest& request,
                         TStreamReaderCallback<TResponse> callback,
                         typename TStreamRequestReadProcessor<TStub, TRequest, TResponse>::TAsyncRequest asyncRequest,
                         const TCallMeta& metas = { },
                         IQueueClientContextProvider* provider = nullptr)
    {
        auto processor = MakeIntrusive<TStreamRequestReadProcessor<TStub, TRequest, TResponse>>(std::move(callback));
        processor->ApplyMeta(metas);
        processor->Start(*Stub_, request, std::move(asyncRequest), provider ? provider : Provider_);
    }

private:
    TServiceConnection(std::shared_ptr<grpc::ChannelInterface> ci,
                       IQueueClientContextProvider* provider)
        : Stub_(TGRpcService::NewStub(ci))
        , Provider_(provider)
    {
        Y_ABORT_UNLESS(Provider_, "Connection does not have a queue provider");
    }

    TServiceConnection(TStubsHolder& holder,
                       IQueueClientContextProvider* provider)
        : Stub_(holder.GetOrCreateStub<TStub>())
        , Provider_(provider)
    {
        Y_ABORT_UNLESS(Provider_, "Connection does not have a queue provider");
    }

    std::shared_ptr<TStub> Stub_;
    IQueueClientContextProvider* Provider_;
};

class TGRpcClientLow
    :  public IQueueClientContextProvider
{
    class TContextImpl;
    friend class TContextImpl;

    enum ECqState : TAtomicBase {
        WORKING = 0,
        STOP_SILENT = 1,
        STOP_EXPLICIT = 2,
    };

public:
    explicit TGRpcClientLow(size_t numWorkerThread = DEFAULT_NUM_THREADS, bool useCompletionQueuePerThread = false);
    ~TGRpcClientLow();

    // Tries to stop all currently running requests (via their stop callbacks)
    // Will shutdown CQ and drain events once all requests have finished
    // No new requests may be started after this call
    void Stop(bool wait = false);

    // Waits until all currently running requests finish execution
    void WaitIdle();

    inline bool IsStopping() const {
        switch (GetCqState()) {
            case WORKING:
                return false;
            case STOP_SILENT:
            case STOP_EXPLICIT:
                return true;
        }

        Y_UNREACHABLE();
    }

    IQueueClientContextPtr CreateContext() override;

    template<typename TGRpcService>
    std::unique_ptr<TServiceConnection<TGRpcService>> CreateGRpcServiceConnection(const TGRpcClientConfig& config) {
        return std::unique_ptr<TServiceConnection<TGRpcService>>(new TServiceConnection<TGRpcService>(CreateChannelInterface(config), this));
    }

    template<typename TGRpcService>
    std::unique_ptr<TServiceConnection<TGRpcService>> CreateGRpcServiceConnection(TStubsHolder& holder) {
        return std::unique_ptr<TServiceConnection<TGRpcService>>(new TServiceConnection<TGRpcService>(holder, this));
    }

    // Tests only, not thread-safe
    void AddWorkerThreadForTest();

private:
    using IThreadRef = std::unique_ptr<IThreadFactory::IThread>;
    using CompletionQueueRef = std::unique_ptr<grpc::CompletionQueue>;
    void Init(size_t numWorkerThread);

    inline ECqState GetCqState() const { return (ECqState) AtomicGet(CqState_); }
    inline void SetCqState(ECqState state) { AtomicSet(CqState_, state); }

    void StopInternal(bool silent);
    void WaitInternal();

    void ForgetContext(TContextImpl* context);

private:
    bool UseCompletionQueuePerThread_;
    std::vector<CompletionQueueRef> CQS_;
    std::vector<IThreadRef> WorkerThreads_;
    TAtomic CqState_ = -1;

    std::mutex Mtx_;
    std::condition_variable ContextsEmpty_;
    std::unordered_set<TContextImpl*> Contexts_;

    std::mutex JoinMutex_;
};

} // namespace NGRpc
