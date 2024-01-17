#include "coordination.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <util/generic/deque.h>
#include <util/random/entropy.h>

namespace NYdb {
namespace NCoordination {

using NThreading::TFuture;
using NThreading::TPromise;
using NThreading::NewPromise;
using NYdbGrpc::TQueueClientFixedEvent;

namespace {

////////////////////////////////////////////////////////////////////////////////

template<class T>
using TResultPromise = TPromise<TResult<T>>;

template<class T>
inline TResultPromise<T> NewResultPromise() {
    return NewPromise<TResult<T>>();
}

////////////////////////////////////////////////////////////////////////////////

template<class Settings>
void ConvertSettingsToProtoConfig(
    const Settings& settings,
    Ydb::Coordination::Config* config)
{
    if (settings.SelfCheckPeriod_) {
        config->set_self_check_period_millis(settings.SelfCheckPeriod_->MilliSeconds());
    }
    if (settings.SessionGracePeriod_) {
        config->set_session_grace_period_millis(settings.SessionGracePeriod_->MilliSeconds());
    }
    if (settings.ReadConsistencyMode_ != EConsistencyMode::UNSET) {
        config->set_read_consistency_mode(static_cast<Ydb::Coordination::ConsistencyMode>(settings.ReadConsistencyMode_));
    }
    if (settings.AttachConsistencyMode_ != EConsistencyMode::UNSET) {
        config->set_attach_consistency_mode(static_cast<Ydb::Coordination::ConsistencyMode>(settings.AttachConsistencyMode_));
    }
    if (settings.RateLimiterCountersMode_ != ERateLimiterCountersMode::UNSET) {
        config->set_rate_limiter_counters_mode(static_cast<Ydb::Coordination::RateLimiterCountersMode>(settings.RateLimiterCountersMode_));
    }
}

////////////////////////////////////////////////////////////////////////////////

TString GenerateProtectionKey(size_t size) {
    TString key;
    if (size > 0) {
        key.resize(size);
        EntropyPool().Read(key.Detach(), size);
    }
    return key;
}

}

////////////////////////////////////////////////////////////////////////////////

struct TNodeDescription::TImpl {
    TImpl(const Ydb::Coordination::DescribeNodeResult& desc) {
        auto& config = desc.config();
        if (config.self_check_period_millis()) {
            SelfCheckPeriod_ = TDuration::MilliSeconds(config.self_check_period_millis());
        }
        if (config.session_grace_period_millis()) {
            SessionGracePeriod_ = TDuration::MilliSeconds(config.session_grace_period_millis());
        }
        ReadConsistencyMode_ = static_cast<EConsistencyMode>(config.read_consistency_mode());
        AttachConsistencyMode_ = static_cast<EConsistencyMode>(config.attach_consistency_mode());
        RateLimiterCountersMode_ = static_cast<ERateLimiterCountersMode>(config.rate_limiter_counters_mode());
        Owner_ = desc.self().owner();
        PermissionToSchemeEntry(desc.self().effective_permissions(), &EffectivePermissions_);
        Proto_ = desc;
    }

    TMaybe<TDuration> SelfCheckPeriod_;
    TMaybe<TDuration> SessionGracePeriod_;
    EConsistencyMode ReadConsistencyMode_;
    EConsistencyMode AttachConsistencyMode_;
    ERateLimiterCountersMode RateLimiterCountersMode_;
    TString Owner_;
    TVector<NScheme::TPermissions> EffectivePermissions_;
    Ydb::Coordination::DescribeNodeResult Proto_;
};

TNodeDescription::TNodeDescription(
        const Ydb::Coordination::DescribeNodeResult& desc)
    : Impl_(new TImpl(desc))
{ }

const TMaybe<TDuration>& TNodeDescription::GetSelfCheckPeriod() const {
    return Impl_->SelfCheckPeriod_;
}

const TMaybe<TDuration>& TNodeDescription::GetSessionGracePeriod() const {
    return Impl_->SessionGracePeriod_;
}

EConsistencyMode TNodeDescription::GetReadConsistencyMode() const {
    return Impl_->ReadConsistencyMode_;
}

EConsistencyMode TNodeDescription::GetAttachConsistencyMode() const {
    return Impl_->AttachConsistencyMode_;
}

ERateLimiterCountersMode TNodeDescription::GetRateLimiterCountersMode() const {
    return Impl_->RateLimiterCountersMode_;
}

const TString& TNodeDescription::GetOwner() const {
    return Impl_->Owner_;
}

const TVector<NScheme::TPermissions>& TNodeDescription::GetEffectivePermissions() const {
    return Impl_->EffectivePermissions_;
}

const Ydb::Coordination::DescribeNodeResult& TNodeDescription::GetProto() const {
    return Impl_->Proto_;
}

////////////////////////////////////////////////////////////////////////////////

TSemaphoreSession::TSemaphoreSession() {
    OrderId_ = 0;
    SessionId_ = 0;
    Count_ = 0;
}

TSemaphoreSession::TSemaphoreSession(
        const Ydb::Coordination::SemaphoreSession& desc)
{
    OrderId_ = desc.order_id();
    SessionId_ = desc.session_id();
    Timeout_ = desc.timeout_millis() == Max<ui64>() ? TDuration::Max() : TDuration::MilliSeconds(desc.timeout_millis());
    Count_ = desc.count();
    Data_ = desc.data();
}

TSemaphoreDescription::TSemaphoreDescription() {
    Count_ = 0;
    Limit_ = 0;
    IsEphemeral_ = false;
}

TSemaphoreDescription::TSemaphoreDescription(
        const Ydb::Coordination::SemaphoreDescription& desc)
{
    Name_ = desc.name();
    Data_ = desc.data();
    Count_ = desc.count();
    Limit_ = desc.limit();
    Owners_.reserve(desc.ownersSize());
    for (const auto& owner : desc.owners()) {
        Owners_.emplace_back(owner);
    }
    Waiters_.reserve(desc.waitersSize());
    for (const auto& waiter : desc.waiters()) {
        Waiters_.emplace_back(waiter);
    }
    IsEphemeral_ = desc.ephemeral();
}

////////////////////////////////////////////////////////////////////////////////

class TSessionContext : public TThrRefBase {
    using TPtr = TIntrusivePtr<TSessionContext>;
    using TService = Ydb::Coordination::V1::CoordinationService;
    using TRequest = Ydb::Coordination::SessionRequest;
    using TResponse = Ydb::Coordination::SessionResponse;
    using TGrpcStatus = NYdbGrpc::TGrpcStatus;
    using IProcessor = NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>;

    friend class TSession::TImpl;

public:
    TSessionContext(
            TGRpcConnectionsImpl* connections,
            TDbDriverStatePtr dbState,
            const TString& path,
            const TSessionSettings& settings)
        : Connections_(connections)
        , DbDriverState_(dbState)
        , Path_(path)
        , Settings_(settings)
        , ProtectionKey_(GenerateProtectionKey(8))
        , SessionPromise(NewPromise<TSessionResult>())
        , SessionFuture(SessionPromise.GetFuture())
    { }

    TAsyncSessionResult TakeStartResult() {
        TAsyncSessionResult result;
        result.Swap(SessionFuture);
        Y_ABORT_UNLESS(result.Initialized(), "TakeStartResult cannot be called more than once");
        return result;
    }

    void Start(IQueueClientContextProvider* provider) {
        auto context = provider->CreateContext();
        if (!context) {
            auto status = MakeStatus(EStatus::CLIENT_CANCELLED, "Client is stopped");
            auto promise = std::move(SessionPromise);
            promise.SetValue(TSessionResult(std::move(status)));
            return;
        }
        with_lock (Lock) {
            LocalContext = context;
            ConnectionState = EConnectionState::CONNECTING;
        }
        DoConnectRequest(context);
        context->SubscribeStop([self = TPtr(this)] {
            self->Stop();
        });
    }

    void Stop() {
        with_lock (Lock) {
            IsStopping = true;
            if (!CurrentFailure) {
                SetCurrentFailure(
                    TPlainStatus(EStatus::CLIENT_CANCELLED, "Client is stopped"));
            }
            if (Processor) {
                Processor->Cancel();
            }
            if (SleepContext) {
                SleepContext->Cancel();
            }
        }
    }

private:
    enum ESemaphoreOpType {
        SEM_OP_ACQUIRE,
        SEM_OP_RELEASE,
    };

    struct TSemaphoreOp : public TSimpleRefCount<TSemaphoreOp> {
        TInstant SendTimestamp;
        ui64 ReqId = 0;
        TResultPromise<bool> Promise = NewResultPromise<bool>();
        const ESemaphoreOpType OpType;

        TSemaphoreOp(ESemaphoreOpType opType)
            : OpType(opType)
        {}

        virtual ~TSemaphoreOp() = default;
        virtual void FillRequest(TRequest& req, const TString& name) const = 0;
    };

    struct TSemaphoreAcquireOp : public TSemaphoreOp {
        TAcquireSemaphoreSettings Settings;
        TInstant Deadline;

        explicit TSemaphoreAcquireOp(const TAcquireSemaphoreSettings& settings)
            : TSemaphoreOp(SEM_OP_ACQUIRE)
            , Settings(settings)
            , Deadline(Settings.Timeout_.ToDeadLine())
        { }

        ui64 GetTimeoutMillisLeft() const {
            if (Deadline == TInstant::Max()) {
                return Max<ui64>();
            }
            return (Deadline - TInstant::Now()).MilliSeconds();
        }

        void FillRequest(TRequest& req, const TString& name) const override {
            auto& inner = *req.mutable_acquire_semaphore();
            inner.set_req_id(ReqId);
            inner.set_name(name);
            inner.set_count(Settings.Count_);
            inner.set_timeout_millis(GetTimeoutMillisLeft());
            inner.set_data(Settings.Data_);
            inner.set_ephemeral(Settings.Ephemeral_);
        }
    };

    struct TSemaphoreReleaseOp : public TSemaphoreOp {
        TSemaphoreReleaseOp()
            : TSemaphoreOp(SEM_OP_RELEASE)
        {}

        void FillRequest(TRequest& req, const TString& name) const override {
            auto& inner = *req.mutable_release_semaphore();
            inner.set_req_id(ReqId);
            inner.set_name(name);
        }
    };

    struct TSemaphoreState {
        TString Name;
        TIntrusivePtr<TSemaphoreOp> LastSentOp;
        TIntrusivePtr<TSemaphoreOp> LastAckedOp;
        THashMap<ui64, TIntrusivePtr<TSemaphoreOp>> WaitingOps;
        TDeque<TIntrusivePtr<TSemaphoreOp>> OpQueue;
        bool Restoring = false;

        bool IsEmpty() const {
            return !LastSentOp
                && !LastAckedOp
                && WaitingOps.empty()
                && OpQueue.empty();
        }
    };

    struct TSimpleOp {
        TInstant SendTimestamp;

        virtual ~TSimpleOp() = default;
        virtual void FillRequest(TRequest& req, ui64 reqId) const = 0;
        virtual void SetFailure(const TStatus& status) = 0;
    };

    struct TPingOp : public TSimpleOp {
        TResultPromise<void> Promise = NewResultPromise<void>();

        TPingOp() = default;

        void FillRequest(TRequest& req, ui64 reqId) const override {
            auto& inner = *req.mutable_ping();
            inner.set_opaque(reqId);
        }

        void SetFailure(const TStatus& status) override {
            Promise.SetValue(TResult<void>(status));
        }
    };

    struct TDescribeSemaphoreOp : public TSimpleOp {
        const TString Name;
        TDescribeSemaphoreSettings Settings;
        TPromise<TDescribeSemaphoreResult> Promise = NewPromise<TDescribeSemaphoreResult>();

        TDescribeSemaphoreOp(const TString& name, const TDescribeSemaphoreSettings& settings)
            : Name(name)
            , Settings(settings)
        {}

        void FillRequest(TRequest& req, ui64 reqId) const override {
            auto& inner = *req.mutable_describe_semaphore();
            inner.set_req_id(reqId);
            inner.set_name(Name);
            inner.set_include_owners(Settings.IncludeOwners_);
            inner.set_include_waiters(Settings.IncludeWaiters_);
            inner.set_watch_data(Settings.WatchData_);
            inner.set_watch_owners(Settings.WatchOwners_);
        }

        void SetFailure(const TStatus& status) override {
            if (Promise.Initialized()) {
                Promise.SetValue(TDescribeSemaphoreResult(status));
            } else if (Settings.OnChanged_) {
                std::function<void(bool)> callback;
                callback.swap(Settings.OnChanged_);
                callback(false);
            }
        }
    };

    struct TCreateSemaphoreOp : public TSimpleOp {
        const TString Name;
        const ui64 Limit;
        const TString Data;
        TResultPromise<void> Promise = NewResultPromise<void>();

        TCreateSemaphoreOp(const TString& name, ui64 limit, const TString& data)
            : Name(name)
            , Limit(limit)
            , Data(data)
        { }

        void FillRequest(TRequest& req, ui64 reqId) const override {
            auto& inner = *req.mutable_create_semaphore();
            inner.set_req_id(reqId);
            inner.set_name(Name);
            inner.set_limit(Limit);
            inner.set_data(Data);
        }

        void SetFailure(const TStatus& status) override {
            Promise.SetValue(TResult<void>(status));
        }
    };

    struct TUpdateSemaphoreOp : public TSimpleOp {
        const TString Name;
        const TString Data;
        TResultPromise<void> Promise = NewResultPromise<void>();

        TUpdateSemaphoreOp(const TString& name, const TString& data)
            : Name(name)
            , Data(data)
        {}

        void FillRequest(TRequest& req, ui64 reqId) const override {
            auto& inner = *req.mutable_update_semaphore();
            inner.set_req_id(reqId);
            inner.set_name(Name);
            inner.set_data(Data);
        }

        void SetFailure(const TStatus& status) override {
            Promise.SetValue(TResult<void>(status));
        }
    };

    struct TDeleteSemaphoreOp : public TSimpleOp {
        const TString Name;
        const bool Force;
        TResultPromise<void> Promise = NewResultPromise<void>();

        TDeleteSemaphoreOp(const TString& name, bool force)
            : Name(name)
            , Force(force)
        {}

        void FillRequest(TRequest& req, ui64 reqId) const override {
            auto& inner = *req.mutable_delete_semaphore();
            inner.set_req_id(reqId);
            inner.set_name(Name);
            inner.set_force(Force);
        }

        void SetFailure(const TStatus& status) override {
            Promise.SetValue(TResult<void>(status));
        }
    };

private:
    ESessionState DoGetSessionState() {
        with_lock (Lock) {
            return SessionState;
        }
    }

    EConnectionState DoGetConnectionState() {
        with_lock (Lock) {
            return ConnectionState;
        }
    }

    TAsyncResult<void> DoClose(bool isAbort) {
        with_lock (Lock) {
            if (!ClosedPromise.Initialized()) {
                ClosedPromise = NewResultPromise<void>();
                if (IsWriteAllowed()) {
                    SendCloseRequest();
                }
            }
            if (isAbort) {
                // Move to the stopping state, but without cancelling the context
                IsStopping = true;
                if (!CurrentFailure) {
                    SetCurrentFailure(
                        TPlainStatus(EStatus::UNAVAILABLE, "Session is aborted"));
                }
                if (SleepContext) {
                    SleepContext->Cancel();
                }
            }
            return ClosedPromise;
        }
    }

    TAsyncResult<void> DoPing() {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<void>();
            }
            auto op = MakeHolder<TPingOp>();
            auto future = op->Promise.GetFuture();
            if (IsWriteAllowed()) {
                DoSendSimpleOp(std::move(op));
            } else {
                PendingRequests.emplace_back(std::move(op));
            }
            return future;
        }
    }

    TAsyncResult<void> DoReconnect() {
        with_lock (Lock) {
            SessionReconnectDelay = TDuration::Zero();
            if (Processor) {
                Processor->Cancel();
            }
            if (SleepContext) {
                SleepContext->Cancel();
            }
            if (IsClosed()) {
                return MakeClosedResult<void>();
            }
            if (!ReconnectPromise.Initialized()) {
                ReconnectPromise = NewResultPromise<void>();
            }
            return ReconnectPromise;
        }
    }

    TAsyncResult<bool> DoAcquireSemaphore(
            const TString& name,
            const TAcquireSemaphoreSettings& settings)
        {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<bool>();
            }
            auto op = MakeIntrusive<TSemaphoreAcquireOp>(settings);
            DoSemaphoreEnqueueOp(name, op);
            return op->Promise;
        }
    }

    TAsyncResult<bool> DoReleaseSemaphore(const TString& name) {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<bool>();
            }
            auto op = MakeIntrusive<TSemaphoreReleaseOp>();
            DoSemaphoreEnqueueOp(name, op);
            return op->Promise;
        }
    }

    TAsyncDescribeSemaphoreResult DoDescribeSemaphore(
            const TString& name,
            const TDescribeSemaphoreSettings& settings)
    {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<TSemaphoreDescription>();
            }
            auto op = MakeHolder<TDescribeSemaphoreOp>(name, settings);
            auto future = op->Promise.GetFuture();
            if (IsWriteAllowed()) {
                DoSendSimpleOp(std::move(op));
            } else {
                PendingRequests.emplace_back(std::move(op));
            }
            return future;
        }
    }

    TAsyncResult<void> DoCreateSemaphore(const TString& name, ui64 limit, const TString& data) {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<void>();
            }
            auto op = MakeHolder<TCreateSemaphoreOp>(name, limit, data);
            auto future = op->Promise.GetFuture();
            if (IsWriteAllowed()) {
                DoSendSimpleOp(std::move(op));
            } else {
                PendingRequests.emplace_back(std::move(op));
            }
            return future;
        }
    }

    TAsyncResult<void> DoUpdateSemaphore(const TString& name, const TString& data) {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<void>();
            }
            auto op = MakeHolder<TUpdateSemaphoreOp>(name, data);
            auto future = op->Promise.GetFuture();
            if (IsWriteAllowed()) {
                DoSendSimpleOp(std::move(op));
            } else {
                PendingRequests.emplace_back(std::move(op));
            }
            return future;
        }
    }

    TAsyncResult<void> DoDeleteSemaphore(const TString& name, bool force) {
        with_lock (Lock) {
            if (IsClosed()) {
                return MakeClosedResult<void>();
            }
            auto op = MakeHolder<TDeleteSemaphoreOp>(name, force);
            auto future = op->Promise.GetFuture();
            if (IsWriteAllowed()) {
                DoSendSimpleOp(std::move(op));
            } else {
                PendingRequests.emplace_back(std::move(op));
            }
            return future;
        }
    }

    TDuration GetConnectTimeout() {
        // Use a separate connect timeout if available
        if (Settings_.ConnectTimeout_) {
            return Settings_.ConnectTimeout_;
        }

        // Use session timeout otherwise
        return Settings_.Timeout_;
    }

    void DoConnectRequest(std::shared_ptr<IQueueClientContext> context) {
        auto connectContext = context->CreateContext();

        auto connectTimeout = GetConnectTimeout();
        auto connectTimeoutContext = connectTimeout != TDuration::Max()
            ? connectContext->CreateContext()
            : nullptr;

        IQueueClientContextPtr prevConnectContext;
        IQueueClientContextPtr prevConnectTimeoutContext;

        with_lock (Lock) {
            prevConnectContext = std::exchange(ConnectContext, connectContext);
            prevConnectTimeoutContext = std::exchange(ConnectTimeoutContext, connectTimeoutContext);
        }

        // Cancel previous connection contexts (if they still exist)
        if (prevConnectTimeoutContext) {
            prevConnectTimeoutContext->Cancel();
        }
        if (prevConnectContext) {
            prevConnectContext->Cancel();
        }

        Connections_->StartBidirectionalStream<TService, TRequest, TResponse>(
            [self = TPtr(this)] (auto status, auto processor) {
                self->OnConnect(std::move(status), std::move(processor));
            },
            &TService::Stub::AsyncSession,
            DbDriverState_,
            TRpcRequestSettings::Make(Settings_),
            std::move(connectContext));

        if (connectTimeoutContext) {
            // N.B. to avoid deadlocks we must schedule timeout outside the lock
            auto timerHandler = [self = TPtr(this), connectTimeoutContext] (bool ok) mutable {
                if (ok) {
                    self->OnConnectTimeout(std::move(connectTimeoutContext));
                }
            };
            Connections_->ScheduleCallback(
                connectTimeout,
                std::move(timerHandler),
                std::move(connectTimeoutContext));
        }
    }

private:
    TPlainStatus MakePlainStatus(
        Ydb::StatusIds::StatusCode protoStatus,
        const NProtoBuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& protoIssues) const
    {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(protoIssues, issues);
        return TPlainStatus(static_cast<EStatus>(protoStatus), std::move(issues));
    }

    TStatus MakeStatus() const {
        return TStatus(TPlainStatus());
    }

    template<class TSource>
    TStatus MakeStatus(TSource&& source) const {
        return TStatus(std::forward<TSource>(source));
    }

    TStatus MakeStatus(EStatus status, NYql::TIssues&& issues) const {
        return TStatus(TPlainStatus(status, std::move(issues)));
    }

    TStatus MakeStatus(EStatus status, const TString& message) const {
        return TStatus(TPlainStatus(status, message));
    }

private:
    bool IsClosed() const {
        return IsStopping || ClosedPromise.Initialized();
    }

    bool IsWriteAllowed() const {
        return Processor &&
            SessionState == ESessionState::ATTACHED &&
            ConnectionState == EConnectionState::CONNECTED;
    }

    template<class TSource>
    void SetCurrentFailure(TSource&& source) {
        Y_ABORT_UNLESS(!CurrentFailure);
        CurrentFailure.Reset(new TStatus(std::forward<TSource>(source)));
    }

    template<class T>
    TAsyncResult<T> MakeClosedResult() const {
        auto promise = NewResultPromise<T>();
        if (CurrentFailure) {
            promise.SetValue(TResult<T>(*CurrentFailure));
        } else {
            auto status = MakeStatus(EStatus::CLIENT_CANCELLED, "Session is closed");
            promise.SetValue(TResult<T>(std::move(status)));
        }
        return promise;
    }

    void SendCloseRequest() {
        Y_ABORT_UNLESS(Processor);
        Y_ABORT_UNLESS(ClosedPromise.Initialized());
        TRequest req;
        auto* stop = req.mutable_session_stop();
        Y_UNUSED(stop);
        Processor->Write(std::move(req));
    }

    void SessionAttached() {
        Y_ABORT_UNLESS(SessionState != ESessionState::ATTACHED);
        Y_ABORT_UNLESS(ConnectionState == EConnectionState::ATTACHING);
        SessionState = ESessionState::ATTACHED;
        ConnectionState = EConnectionState::CONNECTED;
        SessionExpireDeadline.Clear();
        SessionReconnectDelay = TDuration::Zero();
        if (ClosedPromise.Initialized()) {
            // Send the delayed close request, will fail if cancelled
            SendCloseRequest();
        }
        if (IsStopping) {
            // Either cancelled (via shutdown) or aborted (implicit close)
            return;
        }
        for (auto& kv : Semaphores) {
            TSemaphoreState* state = &kv.second;
            if (state->LastSentOp) {
                // Repeat the last sent op
                DoSemaphoreSendOp(state, state->LastSentOp);
                state->Restoring = true;
            } else if (state->LastAckedOp) {
                // Repeat the last acknowledged op
                DoSemaphoreSendOp(state, state->LastAckedOp);
                state->Restoring = true;
            } else {
                state->Restoring = false;
                DoSemaphoreProcessQueue(state);
            }
        }
        for (auto& op : PendingRequests) {
            DoSendSimpleOp(std::move(op));
        }
        PendingRequests.clear();
    }

    void DoSemaphoreSendOp(TSemaphoreState* state, TIntrusivePtr<TSemaphoreOp> op) {
        Y_ABORT_UNLESS(IsWriteAllowed());
        Y_ABORT_UNLESS(op->ReqId > 0);
        Y_ABORT_UNLESS(!state->WaitingOps.contains(op->ReqId));
        SemaphoreByReqId[op->ReqId] = state;
        state->WaitingOps[op->ReqId] = op;
        state->LastSentOp = op;
        TRequest req;
        op->FillRequest(req, state->Name);
        op->SendTimestamp = TInstant::Now();
        Processor->Write(std::move(req));
    }

    void DoSemaphoreProcessQueue(TSemaphoreState* state) {
        if (!state->OpQueue.empty() && !state->LastSentOp && IsWriteAllowed()) {
            auto op = std::move(state->OpQueue.front());
            state->OpQueue.pop_front();
            op->ReqId = NextReqId++;
            DoSemaphoreSendOp(state, std::move(op));
        }
    }

    bool DoSemaphoreProcessResult(
            ui64 reqId, ESemaphoreOpType opType, EStatus status,
            TResultPromise<bool>* supersededPromise,
            TResultPromise<bool>* resultPromise)
    {
        auto* state = SemaphoreByReqId.Value(reqId, nullptr);
        if (!state) {
            return false;
        }
        auto op = std::move(state->WaitingOps[reqId]);
        Y_ABORT_UNLESS(op, "SemaphoreByReqId and WaitingOps desync");
        state->WaitingOps.erase(reqId);
        SemaphoreByReqId.erase(reqId);
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);

        if (op->OpType != opType) {
            return false;
        }

        bool isError = status != EStatus::SUCCESS;

        if (state->LastSentOp == op) {
            // Result received without getting a wait ack first
            state->LastSentOp = {};
            if (state->LastAckedOp == op || !isError) {
                // This was a post-reconnect retry, or the op succeeded
                if (state->Restoring && state->LastAckedOp && state->LastAckedOp != op) {
                    // The last acknowledged op has never been sent, remember its promise
                    supersededPromise->Swap(state->LastAckedOp->Promise);
                }
                state->LastAckedOp = {};
            }
            if (state->Restoring && state->LastAckedOp) {
                // Unconfirmed op failed, retry the last confirmed op
                if (IsWriteAllowed()) {
                    DoSemaphoreSendOp(state, state->LastAckedOp);
                }
            } else {
                // Try sending whatever else is ready in the queue
                DoSemaphoreProcessQueue(state);
            }
        } else if (state->LastAckedOp == op) {
            // Result received after a wait ack
            state->LastAckedOp = {};
        }

        state->Restoring = false;
        resultPromise->Swap(op->Promise);

        if (state->IsEmpty()) {
            // Forget useless semaphore entries
            TString name = state->Name;
            Semaphores.erase(name);
        }

        return true;
    }

    void DoSemaphoreEnqueueOp(const TString& name, TIntrusivePtr<TSemaphoreOp> op) {
        TSemaphoreState* state = Semaphores.FindPtr(name);
        if (!state) {
            state = &Semaphores[name];
            state->Name = name;
        }
        state->OpQueue.emplace_back(op);
        DoSemaphoreProcessQueue(state);
    }

    ui64 DoSendSimpleOp(THolder<TSimpleOp> op) {
        Y_ABORT_UNLESS(IsWriteAllowed());
        ui64 reqId = NextReqId++;
        TRequest req;
        op->FillRequest(req, reqId);
        op->SendTimestamp = TInstant::Now();
        Processor->Write(std::move(req));
        SentRequests[reqId] = std::move(op);
        return reqId;
    }

    template<class TOperation>
    TOperation* FindSentRequest(ui64 reqId) const {
        auto it = SentRequests.find(reqId);
        if (it == SentRequests.end()) {
            return nullptr;
        }
        return dynamic_cast<TOperation*>(it->second.Get());
    }

private:
    void OnProcessorStatus(TStatus status) {
        std::shared_ptr<IQueueClientContext> context;
        TDbDriverStatePtr dbDriverState;

        bool stopped;
        bool expired = false;
        bool notifyExpired = false;
        TPromise<TSessionResult> sessionPromise;
        TResultPromise<void> reconnectPromise;
        TDeque<TResultPromise<bool>> abortedSemaphoreOps;
        TDeque<TResultPromise<bool>> failedSemaphoreOps;
        TDeque<THolder<TSimpleOp>> failedSimpleOps;
        TResultPromise<void> closePromise;

        with_lock (Lock) {
            Processor.Reset();

            Y_ABORT_UNLESS(LocalContext, "Processing event without a valid context");

            // We must have detached by the time status is processed
            Y_ABORT_UNLESS(SessionState != ESessionState::ATTACHED);

            stopped = IsStopping;

            if (SessionPromise.Initialized()) {
                // Session was never established, no retries
                sessionPromise.Swap(SessionPromise);
                stopped = true;
            }

            if (!stopped) {
                if (SessionTimeout) {
                    // No retries after timeout
                    stopped = true;
                } else if (!SessionExpireDeadline) {
                    // We will only keep retrying until this deadline expires
                    auto deadline = SessionLastKnownGoodTimestamp +
                        Settings_.Timeout_ * Settings_.ReconnectSessionTimeoutMultiplier_;
                    SessionExpireDeadline = deadline;
                } else if (*SessionExpireDeadline <= TInstant::Now()) {
                    // Stop retrying right now
                    SessionTimeout = true;
                    stopped = true;
                }
            }

            if (stopped) {
                context = std::move(LocalContext);
                dbDriverState = std::move(DbDriverState_);
                expired = SessionState == ESessionState::EXPIRED;
                IsStopping = true;
                ConnectionState = EConnectionState::STOPPED;
                if (!ClosedPromise.Initialized()) {
                    ClosedPromise = NewResultPromise<void>();
                }
                closePromise = ClosedPromise;
                if (ReconnectPromise.Initialized()) {
                    ReconnectPromise.Swap(reconnectPromise);
                }
                if (SessionId != 0 && SessionTimeout && !expired) {
                    // N.B. looking at common usage patterns users expect that
                    // session eventually reaches the expired state, and write
                    // buggy code that wouldn't work otherwise. Even though we
                    // don't really know if session has expired or not, we move
                    // to expired state as a cortesy to users.
                    SessionState = ESessionState::EXPIRED;
                    notifyExpired = true;
                }
            } else {
                context = LocalContext;
                ConnectionState = EConnectionState::DISCONNECTED;
            }

            // Replace status with a previously known failure
            if (CurrentFailure) {
                if (stopped) {
                    status = *CurrentFailure;
                } else {
                    status = std::move(*CurrentFailure);
                    CurrentFailure.Reset();
                }
            }

            // Handle all waiting ops
            for (auto& kv : Semaphores) {
                TSemaphoreState* state = &kv.second;
                for (auto& wkv : state->WaitingOps) {
                    auto op = wkv.second;
                    if (stopped) {
                        if (state->LastSentOp == op) {
                            state->LastSentOp = {};
                        }
                        if (state->LastAckedOp == op) {
                            state->LastAckedOp = {};
                        }
                        failedSemaphoreOps.emplace_back(std::move(op->Promise));
                    } else if (op != state->LastSentOp && op != state->LastAckedOp) {
                        // This operation was acknowledged, but will never be retried
                        // We already know it was superseded, we just haven't got a reply yet
                        abortedSemaphoreOps.emplace_back(std::move(op->Promise));
                    }
                }
                state->WaitingOps.clear();
                if (stopped) {
                    if (state->LastAckedOp && state->LastAckedOp != state->LastSentOp) {
                        failedSemaphoreOps.emplace_back(std::move(state->LastAckedOp->Promise));
                        state->LastAckedOp = {};
                    }
                    if (state->LastSentOp) {
                        failedSemaphoreOps.emplace_back(std::move(state->LastSentOp->Promise));
                        state->LastSentOp = {};
                    }
                    for (auto& op : state->OpQueue) {
                        failedSemaphoreOps.emplace_back(std::move(op->Promise));
                    }
                    state->OpQueue.clear();
                }
            }
            SemaphoreByReqId.clear();
            if (stopped) {
                Semaphores.clear();
            }

            for (auto& kv : SentRequests) {
                failedSimpleOps.emplace_back(std::move(kv.second));
            }
            SentRequests.clear();

            if (stopped) {
                for (auto& op : PendingRequests) {
                    failedSimpleOps.emplace_back(std::move(op));
                }
                PendingRequests.clear();
            }
        }

        if (status.IsSuccess()) {
            status = MakeStatus(EStatus::CLIENT_INTERNAL_ERROR, "Unexpected session request success");
        }

        if (sessionPromise.Initialized()) {
            sessionPromise.SetValue(TSessionResult(status));
        }

        if (abortedSemaphoreOps) {
            auto aborted = MakeStatus(EStatus::ABORTED,
                "Operation superseded by another request, true result has been lost");
            for (auto& promise : abortedSemaphoreOps) {
                promise.SetValue(TResult<bool>(aborted, false));
            }
        }

        for (auto& promise : failedSemaphoreOps) {
            promise.SetValue(TResult<bool>(status, false));
        }

        for (auto& op : failedSimpleOps) {
            op->SetFailure(status);
        }

        if (closePromise.Initialized()) {
            closePromise.SetValue(TResult<void>(expired ? MakeStatus() : status));
        }

        if (stopped) {
            if (notifyExpired && Settings_.OnStateChanged_) {
                Settings_.OnStateChanged_(ESessionState::EXPIRED);
            }
            if (Settings_.OnStopped_) {
                Settings_.OnStopped_();
            }
            return;
        }

        TDuration sleepDuration;
        IQueueClientContextPtr sleepContext;
        with_lock (Lock) {
            if (!IsStopping) {
                // Session object still active, calculate sleep duration
                sleepDuration = Min(SessionReconnectDelay, *SessionExpireDeadline - TInstant::Now());
                if (SessionReconnectDelay == TDuration::Zero()) {
                    SessionReconnectDelay = Settings_.ReconnectBackoffDelay_;
                } else {
                    SessionReconnectDelay = Min(
                        SessionReconnectDelay * Settings_.ReconnectBackoffMultiplier_,
                        Settings_.Timeout_);
                }
            } else {
                // IsStopping == true means session was stopped by the client
                // during all the processing above. We use a zero-sleep duration
                // to reschedule this error processing handler.
                sleepDuration = TDuration::Zero();
            }

            Y_ABORT_UNLESS(!SleepContext, "Unexpected multiple concurrent sleeps scheduled");
            SleepContext = sleepContext = context->CreateContext();
        }

        auto sleepHandler = [self = TPtr(this)] (bool ok) mutable {
            // N.B. OnSleepFinished is always called
            self->OnSleepFinished(ok);
        };
        Connections_->ScheduleCallback(
            sleepDuration,
            std::move(sleepHandler),
            std::move(sleepContext));
    }

    void OnConnectTimeout(IQueueClientContextPtr context) {
        Y_ABORT_UNLESS(context, "Connection timeout context is unexpectedly empty");

        with_lock (Lock) {
            if (ConnectTimeoutContext != context) {
                // Context changed or dropped, ignore
                return;
            }

            ConnectTimeoutContext.reset();

            // Timer succeeded and still current, cancel connection attempt
            Y_ABORT_UNLESS(ConnectContext, "Connection context is unexpectedly empty");
            ConnectContext->Cancel();
            ConnectContext.reset();

            SessionTimeout = true;
            if (!CurrentFailure) {
                SetCurrentFailure(
                    TPlainStatus(EStatus::TIMEOUT, "Connection request timed out"));
            }
        }
    }

    void OnConnect(TPlainStatus plain, IProcessor::TPtr processor) {
        bool hadTimeout = false;

        with_lock (Lock) {
            if (SessionTimeout) {
                // OnConnectTimeout was called first
                hadTimeout = true;
            }

            // Reset context pointers, no longer interested in these
            ConnectTimeoutContext.reset();
            ConnectContext.reset();
        }

        if (hadTimeout) {
            // Even if connection succeeded it's likely to break soon, as
            // the context used for stream creation has been cancelled already
            if (processor) {
                processor->Cancel();
            }
            OnProcessorStatus(MakeStatus(EStatus::TIMEOUT, "Connection request timed out"));
            return;
        }

        if (!plain.Ok()) {
            OnProcessorStatus(MakeStatus(std::move(plain)));
            return;
        }

        ui64 seqNo;
        ui64 sessionId;

        TDuration timeout = Settings_.Timeout_;
        IQueueClientContextPtr timeoutContext;

        TResultPromise<void> reconnectPromise;
        with_lock (Lock) {
            Processor = processor;
            ConnectionState = EConnectionState::ATTACHING;
            if (ReconnectPromise.Initialized()) {
                ReconnectPromise.Swap(reconnectPromise);
            }

            seqNo = ++SessionSeqNo;
            sessionId = SessionId;

            SessionStartTimestamp = TInstant::Now();

            if (timeout != TDuration::Max()) {
                // If this is not the first time we start a session, assume worst
                // case that after 2x session timeout there's no point in trying
                // to restore it.
                if (SessionLastKnownGoodTimestamp) {
                    auto maxDeadline = SessionLastKnownGoodTimestamp
                            + Settings_.Timeout_ * Settings_.ReconnectSessionTimeoutMultiplier_;
                    auto maxTimeout = maxDeadline - SessionStartTimestamp;
                    if (timeout > maxTimeout) {
                        timeout = maxTimeout;
                    }
                }

                SessionStartTimeoutContext = timeoutContext = LocalContext->CreateContext();
            }
        }

        if (reconnectPromise.Initialized()) {
            // Set promise before issuing any requests on the connection
            // This way there will be no races between promise and state changes
            // However we must be ready for session to become aborted during this callback
            reconnectPromise.SetValue(TResult<void>(MakeStatus()));
        }

        // Prepare and write the initial request
        // It may be ignored if Close is called concurrently
        {
            TRequest req;
            auto* start = req.mutable_session_start();
            start->set_seq_no(seqNo);
            start->set_session_id(sessionId);
            start->set_path(Path_);
            start->set_timeout_millis(Settings_.Timeout_ != TDuration::Max() ?
                Settings_.Timeout_.MilliSeconds() : Max<ui64>());
            start->set_description(Settings_.Description_);
            start->set_protection_key(ProtectionKey_);
            processor->Write(std::move(req));
        }

        // Start reading responses
        Response.Reset(new TResponse);
        processor->Read(
            Response.Get(),
            [self = TPtr(this)] (auto status) {
                self->OnRead(std::move(status));
            });

        if (timeoutContext) {
            // N.B. to avoid deadlocks we must schedule timeout outside locks
            auto timeoutHandler = [self = TPtr(this), timeoutContext] (bool ok) mutable {
                if (ok) {
                    self->OnSessionStartTimeout(std::move(timeoutContext));
                }
            };
            Connections_->ScheduleCallback(
                timeout,
                std::move(timeoutHandler),
                std::move(timeoutContext));
        }
    }

    enum class ESessionDetachResult {
        Ok,
        Timeout,
    };

    ESessionDetachResult HandleSessionDetach() {
        ESessionDetachResult result = ESessionDetachResult::Ok;

        IQueueClientContextPtr sessionStartTimeoutContext;
        IQueueClientContextPtr sessionSelfPingContext;

        bool detached = false;
        with_lock (Lock) {
            if (SessionState == ESessionState::ATTACHED) {
                SessionState = ESessionState::DETACHED;
                detached = true;
            }

            sessionStartTimeoutContext = std::exchange(SessionStartTimeoutContext, nullptr);
            sessionSelfPingContext = std::exchange(SessionSelfPingContext, nullptr);

            if (SessionTimeout) {
                result = ESessionDetachResult::Timeout;
            }
        }

        if (detached && Settings_.OnStateChanged_) {
            Settings_.OnStateChanged_(ESessionState::DETACHED);
        }

        if (sessionStartTimeoutContext) {
            // Session start timeout no longer relevant
            sessionStartTimeoutContext->Cancel();
        }

        if (sessionSelfPingContext) {
            // Session pings are no longer relevant
            sessionSelfPingContext->Cancel();
        }

        return result;
    }

    void OnRead(TGrpcStatus grpcStatus) {
        if (!grpcStatus.Ok()) {
            Response.Reset();
            switch (HandleSessionDetach()) {
                case ESessionDetachResult::Ok:
                    // Report grpc status to client
                    OnProcessorStatus(MakeStatus(TPlainStatus(std::move(grpcStatus))));
                    break;
                case ESessionDetachResult::Timeout:
                    // Original grpc status is irrelevant due to timeout
                    OnProcessorStatus(MakeStatus(EStatus::TIMEOUT, "Session timed out"));
                    break;
            }
            return;
        }

        IProcessor::TPtr processor;
        with_lock (Lock) {
            processor = Processor;
        }
        Y_ABORT_UNLESS(processor, "Processor is missing for some reason");

        if (ProcessResponse(processor)) {
            // Start reading the next response
            Response.Reset(new TResponse);
            processor->Read(
                Response.Get(),
                [self = TPtr(this)] (auto status) {
                    self->OnRead(std::move(status));
                });
        } else {
            // Stop reading responses
            Response.Reset();
            processor->Finish([self = TPtr(this)] (auto status) {
                self->OnFinish(std::move(status));
            });
        }
    }

    void OnFinish(TGrpcStatus grpcStatus) {
        OnProcessorStatus(MakeStatus(TPlainStatus(std::move(grpcStatus))));
    }

    void OnSleepFinished(bool ok) {
        // N.B. even if sleep was cancelled (ok == false), we must still find
        // the correct next step to move to. Either reconnect immediately
        // without waiting, or finalize the session state.
        Y_UNUSED(ok);

        IQueueClientContextPtr sleepContext;

        bool stopping;
        std::shared_ptr<IQueueClientContext> context;

        with_lock (Lock) {
            sleepContext.swap(SleepContext);
            Y_ABORT_UNLESS(sleepContext, "Unexpected missing SleepContext in OnSleepFinished");
            if (!(stopping = IsStopping)) {
                ConnectionState = EConnectionState::CONNECTING;
                context = LocalContext;
                Y_ABORT_UNLESS(context);
            }
        }

        // Call destructor as soon as possible
        sleepContext.reset();

        if (stopping) {
            OnProcessorStatus(MakeStatus(EStatus::CLIENT_INTERNAL_ERROR, "Session stopped without an error"));
        } else {
            DoConnectRequest(std::move(context));
        }
    }

private:
    void UpdateLastKnownGoodTimestampLocked(TInstant timestamp) {
        SessionLastKnownGoodTimestamp = Max(SessionLastKnownGoodTimestamp, timestamp);
    }

    void OnSelfPingTimer(IQueueClientContextPtr context) {
        TInstant nextTimerTimestamp;

        with_lock (Lock) {
            if (SessionSelfPingContext != context) {
                // Context changed or dropped, ignore
                return;
            }

            if (!IsWriteAllowed()) {
                // Not in a pingable state, ignore
                SessionSelfPingContext.reset();
                return;
            }

            if (auto* op = FindSentRequest<TPingOp>(SessionSelfPingReqId)) {
                // We have an active request which has not been answered yet
                // Cancel everything, it's a timeout :(
                SessionSelfPingContext.reset();

                SessionTimeout = true;
                if (!CurrentFailure) {
                    SetCurrentFailure(
                        TPlainStatus(EStatus::TIMEOUT, "Session ping request timed out"));
                }

                Processor->Cancel();

                return;
            }

            auto now = TInstant::Now();
            auto half = SessionLastKnownGoodTimestamp + Settings_.Timeout_ / 2;
            if (now < half) {
                // Session has enough activity on its own
                // We want to send the next ping 2/3 of the way towards timeout
                nextTimerTimestamp = SessionLastKnownGoodTimestamp + Settings_.Timeout_ * (2.0 / 3.0);
            } else {
                // Send a new ping request right now
                SessionSelfPingReqId = DoSendSimpleOp(MakeHolder<TPingOp>());
                // We want to wait until either:
                // 1. Expected session timeout from the client point of view
                // 2. At least a quarter of session timeout from current time
                auto expectedSessionDeadline = SessionLastKnownGoodTimestamp + Settings_.Timeout_;
                auto minimalWaitDeadline = now + Settings_.Timeout_ / 4;
                nextTimerTimestamp = Max(expectedSessionDeadline, minimalWaitDeadline);
            }
        }

        auto handler = [self = TPtr(this), context] (bool ok) mutable {
            if (ok) {
                self->OnSelfPingTimer(std::move(context));
            }
        };
        Connections_->ScheduleCallback(
            nextTimerTimestamp - TInstant::Now(),
            std::move(handler),
            std::move(context));
    }

    bool ProcessResponse(const IProcessor::TPtr& processor) {
        switch (Response->response_case()) {
            case TResponse::RESPONSE_NOT_SET: {
                Y_ABORT("Unexpected empty response received");
            }
            case TResponse::kPing: {
                const auto& source = Response->ping();
                TRequest req;
                req.mutable_pong()->set_opaque(source.opaque());
                processor->Write(std::move(req));
                return true;
            }
            case TResponse::kPong: {
                const auto& source = Response->pong();
                const ui64 reqId = source.opaque();
                TResultPromise<void> replyPromise;
                with_lock (Lock) {
                    if (auto* op = FindSentRequest<TPingOp>(reqId)) {
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        replyPromise.Swap(op->Promise);
                        SentRequests.erase(reqId);
                    }
                }
                if (replyPromise.Initialized()) {
                    replyPromise.SetValue(TResult<void>(MakeStatus()));
                }
                return true;
            }
            case TResponse::kFailure: {
                const auto& source = Response->failure();
                auto plain = MakePlainStatus(source.status(), source.issues());
                bool expired = false;
                bool detached = false;
                with_lock (Lock) {
                    if (plain.Status == EStatus::SESSION_EXPIRED ||
                        plain.Status == EStatus::UNAUTHORIZED ||
                        plain.Status == EStatus::NOT_FOUND)
                    {
                        // Fatal errors move to the stopping state
                        IsStopping = true;
                        if (SessionId != 0 && SessionState != ESessionState::EXPIRED) {
                            SessionState = ESessionState::EXPIRED;
                            expired = true;
                        }
                    }
                    if (SessionState == ESessionState::ATTACHED) {
                        SessionState = ESessionState::DETACHED;
                        detached = true;
                    }
                    if (!CurrentFailure) {
                        SetCurrentFailure(std::move(plain));
                    }
                }
                if (expired && Settings_.OnStateChanged_) {
                    Settings_.OnStateChanged_(ESessionState::EXPIRED);
                } else if (detached && Settings_.OnStateChanged_) {
                    Settings_.OnStateChanged_(ESessionState::DETACHED);
                }
                return false;
            }
            case TResponse::kSessionStarted: {
                const auto& source = Response->session_started();
                Y_ABORT_UNLESS(source.session_id() > 0);
                TPromise<TSessionResult> replyPromise;
                IQueueClientContextPtr sessionStartTimeoutContext;
                IQueueClientContextPtr sessionSelfPingContext;
                TInstant selfPingFirstTimestamp;
                with_lock (Lock) {
                    // Remember session id for future reconnects
                    SessionId = source.session_id();
                    if (SessionPromise.Initialized()) {
                        SessionPromise.Swap(replyPromise);
                    }
                    UpdateLastKnownGoodTimestampLocked(SessionStartTimestamp);
                    sessionStartTimeoutContext = std::exchange(SessionStartTimeoutContext, nullptr);

                    SessionAttached();

                    if (Settings_.Timeout_ != TDuration::Max() && !SessionSelfPingContext) {
                        // We want to send the first ping 2/3 of the way towards session timeout
                        SessionSelfPingContext = sessionSelfPingContext = LocalContext->CreateContext();
                        selfPingFirstTimestamp = SessionLastKnownGoodTimestamp
                                + Settings_.Timeout_ * (2.0 / 3.0);
                    }
                }
                if (Settings_.OnStateChanged_) {
                    Settings_.OnStateChanged_(ESessionState::ATTACHED);
                }
                if (replyPromise.Initialized()) {
                    // If there are no listeners session destructor will be immediately called outside of the lock
                    auto session = TSession(this);
                    replyPromise.SetValue(TSessionResult(MakeStatus(), std::move(session)));
                    replyPromise = {};
                }
                if (sessionStartTimeoutContext) {
                    sessionStartTimeoutContext->Cancel();
                }
                if (sessionSelfPingContext) {
                    // N.B. to avoid deadlocks timers must be started outside of locks
                    auto handler = [self = TPtr(this), sessionSelfPingContext] (bool ok) mutable {
                        if (ok) {
                            self->OnSelfPingTimer(std::move(sessionSelfPingContext));
                        }
                    };
                    Connections_->ScheduleCallback(
                        selfPingFirstTimestamp - TInstant::Now(),
                        std::move(handler),
                        std::move(sessionSelfPingContext));
                }
                return true;
            }
            case TResponse::kSessionStopped: {
                bool expired = false;
                with_lock (Lock) {
                    IsStopping = true;
                    if (!ClosedPromise.Initialized()) {
                        ClosedPromise = NewResultPromise<void>();
                    }
                    if (!CurrentFailure) {
                        SetCurrentFailure(TPlainStatus(EStatus::SESSION_EXPIRED, "Session expired"));
                    }
                    if (SessionId != 0 && SessionState != ESessionState::EXPIRED) {
                        SessionState = ESessionState::EXPIRED;
                        expired = true;
                    }
                    Y_ABORT_UNLESS(SessionState != ESessionState::ATTACHED);
                }
                if (expired && Settings_.OnStateChanged_) {
                    Settings_.OnStateChanged_(ESessionState::EXPIRED);
                }
                return false;
            }
            case TResponse::kAcquireSemaphorePending: {
                const auto& source = Response->acquire_semaphore_pending();
                ui64 reqId = source.req_id();
                TResultPromise<bool> supersededPromise;
                std::function<void()> acceptedCallback;
                with_lock (Lock) {
                    if (auto* state = SemaphoreByReqId.Value(reqId, nullptr)) {
                        auto op = state->LastSentOp;
                        Y_ABORT_UNLESS(op && op->ReqId == reqId && op->OpType == SEM_OP_ACQUIRE,
                            "Received AcquireSemaphorePending for an unexpected request");
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        if (state->Restoring && state->LastAckedOp && state->LastAckedOp != op) {
                            // The last acknowledged op hasn't been retried
                            supersededPromise.Swap(state->LastAckedOp->Promise);
                        }
                        if (state->LastAckedOp != op) {
                            // This is the first time request is acknowledged
                            auto* acquire = static_cast<TSemaphoreAcquireOp*>(op.Get());
                            acceptedCallback.swap(acquire->Settings.OnAccepted_);
                        }
                        state->LastAckedOp = op;
                        state->LastSentOp = {};
                        state->Restoring = false;
                        DoSemaphoreProcessQueue(state);
                    }
                }
                if (supersededPromise.Initialized()) {
                    auto status = MakeStatus(EStatus::ABORTED, "Operation superseded by another request");
                    supersededPromise.SetValue(TResult<bool>(std::move(status), false));
                }
                if (acceptedCallback) {
                    acceptedCallback();
                }
                return true;
            }
            case TResponse::kAcquireSemaphoreResult: {
                const auto& source = Response->acquire_semaphore_result();
                const ui64 reqId = source.req_id();
                auto plain = MakePlainStatus(source.status(), source.issues());
                TResultPromise<bool> supersededPromise;
                TResultPromise<bool> resultPromise;
                with_lock (Lock) {
                    DoSemaphoreProcessResult(reqId, SEM_OP_ACQUIRE, plain.Status, &supersededPromise, &resultPromise);
                }
                if (supersededPromise.Initialized()) {
                    auto status = MakeStatus(EStatus::ABORTED, "Operation superseded by another request");
                    supersededPromise.SetValue(TResult<bool>(std::move(status), false));
                }
                if (resultPromise.Initialized()) {
                    resultPromise.SetValue(TResult<bool>(MakeStatus(std::move(plain)), source.acquired()));
                }
                return true;
            }
            case TResponse::kReleaseSemaphoreResult: {
                const auto& source = Response->release_semaphore_result();
                const ui64 reqId = source.req_id();
                auto plain = MakePlainStatus(source.status(), source.issues());
                TResultPromise<bool> supersededPromise;
                TResultPromise<bool> resultPromise;
                with_lock (Lock) {
                    DoSemaphoreProcessResult(reqId, SEM_OP_RELEASE, plain.Status, &supersededPromise, &resultPromise);
                }
                if (supersededPromise.Initialized()) {
                    auto status = MakeStatus(EStatus::ABORTED, "Operation superseded by another request");
                    supersededPromise.SetValue(TResult<bool>(std::move(status), false));
                }
                if (resultPromise.Initialized()) {
                    resultPromise.SetValue(TResult<bool>(MakeStatus(std::move(plain)), source.released()));
                }
                return true;
            }
            case TResponse::kDescribeSemaphoreResult: {
                const auto& source = Response->describe_semaphore_result();
                const ui64 reqId = source.req_id();
                auto plain = MakePlainStatus(source.status(), source.issues());
                TPromise<TDescribeSemaphoreResult> resultPromise;
                with_lock (Lock) {
                    if (auto* op = FindSentRequest<TDescribeSemaphoreOp>(reqId)) {
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        resultPromise.Swap(op->Promise);
                        if (!source.watch_added()) {
                            SentRequests.erase(reqId);
                        }
                    }
                }
                if (resultPromise.Initialized()) {
                    resultPromise.SetValue(
                        TDescribeSemaphoreResult(
                            MakeStatus(std::move(plain)),
                            source.semaphore_description()));
                }
                return true;
            }
            case TResponse::kDescribeSemaphoreChanged: {
                const auto& source = Response->describe_semaphore_changed();
                const ui64 reqId = source.req_id();
                std::function<void(bool)> callback;
                bool triggered = false;
                with_lock (Lock) {
                    if (auto* op = FindSentRequest<TDescribeSemaphoreOp>(reqId)) {
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        callback.swap(op->Settings.OnChanged_);
                        triggered = (op->Settings.WatchData_ && source.data_changed())
                            || (op->Settings.WatchOwners_ && source.owners_changed());
                        SentRequests.erase(reqId);
                    }
                }
                if (callback) {
                    callback(triggered);
                }
                return true;
            }
            case TResponse::kCreateSemaphoreResult: {
                const auto& source = Response->create_semaphore_result();
                const ui64 reqId = source.req_id();
                auto plain = MakePlainStatus(source.status(), source.issues());
                TResultPromise<void> resultPromise;
                with_lock (Lock) {
                    if (auto* op = FindSentRequest<TCreateSemaphoreOp>(reqId)) {
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        resultPromise.Swap(op->Promise);
                        SentRequests.erase(reqId);
                    }
                }
                if (resultPromise.Initialized()) {
                    resultPromise.SetValue(TResult<void>(MakeStatus(std::move(plain))));
                }
                return true;
            }
            case TResponse::kUpdateSemaphoreResult: {
                const auto& source = Response->update_semaphore_result();
                const ui64 reqId = source.req_id();
                auto plain = MakePlainStatus(source.status(), source.issues());
                TResultPromise<void> resultPromise;
                with_lock (Lock) {
                    if (auto* op = FindSentRequest<TUpdateSemaphoreOp>(reqId)) {
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        resultPromise.Swap(op->Promise);
                        SentRequests.erase(reqId);
                    }
                }
                if (resultPromise.Initialized()) {
                    resultPromise.SetValue(TResult<void>(MakeStatus(std::move(plain))));
                }
                return true;
            }
            case TResponse::kDeleteSemaphoreResult: {
                const auto& source = Response->delete_semaphore_result();
                const ui64 reqId = source.req_id();
                auto plain = MakePlainStatus(source.status(), source.issues());
                TResultPromise<void> resultPromise;
                with_lock (Lock) {
                    if (auto* op = FindSentRequest<TDeleteSemaphoreOp>(reqId)) {
                        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
                        resultPromise.Swap(op->Promise);
                        SentRequests.erase(reqId);
                    }
                }
                if (resultPromise.Initialized()) {
                    resultPromise.SetValue(TResult<void>(MakeStatus(std::move(plain))));
                }
                return true;
            }
            case TResponse::kUnsupported6:
            case TResponse::kUnsupported7:
            case TResponse::kUnsupported16:
            case TResponse::kUnsupported17:
            case TResponse::kUnsupported18:
                break;
        }

        Y_ABORT("Unsupported message received");
    }

    void OnSessionStartTimeout(IQueueClientContextPtr context) {
        Y_ABORT_UNLESS(context, "Unexpected empty timeout context");

        with_lock (Lock) {
            if (SessionStartTimeoutContext != context) {
                // Context changed or dropped, ignore
                return;
            }

            SessionStartTimeoutContext.reset();

            // Mark session as timed out
            SessionTimeout = true;
            if (!CurrentFailure) {
                SetCurrentFailure(
                    TPlainStatus(EStatus::TIMEOUT, "Session start request timed out"));
            }

            // We cannot report timeout until OnRead finishes with an error
            if (Processor) {
                Processor->Cancel();
            }
        }
    }

private:
    TGRpcConnectionsImpl* const Connections_;
    TDbDriverStatePtr DbDriverState_;
    const TString Path_;
    const TSessionSettings Settings_;
    const TString ProtectionKey_;

    TAdaptiveLock Lock;

    // Used for shutdown notification and keeping cq threads alive
    IQueueClientContextPtr LocalContext;

    // Used for the initial attach only
    TPromise<TSessionResult> SessionPromise;
    TFuture<TSessionResult> SessionFuture;

    ESessionState SessionState = ESessionState::DETACHED;
    EConnectionState ConnectionState = EConnectionState::DISCONNECTED;

    THolder<TStatus> CurrentFailure;
    TResultPromise<void> ClosedPromise;

    // Used during a connection attempt for a custom timeout
    IQueueClientContextPtr ConnectContext;
    IQueueClientContextPtr ConnectTimeoutContext;

    THashMap<TString, TSemaphoreState> Semaphores;
    THashMap<ui64, TSemaphoreState*> SemaphoreByReqId;
    TDeque<THolder<TSimpleOp>> PendingRequests;
    THashMap<ui64, THolder<TSimpleOp>> SentRequests;
    TResultPromise<void> ReconnectPromise;

    // These are used to manage session timeout
    IQueueClientContextPtr SessionStartTimeoutContext;
    IQueueClientContextPtr SessionSelfPingContext;
    ui64 SessionSelfPingReqId = 0;

    // This will be set to true when session is timed out
    bool SessionTimeout = false;

    // Timestamp of the last session start request
    TInstant SessionStartTimestamp;

    // This is the last timestamp when session was confirmed to be good
    TInstant SessionLastKnownGoodTimestamp;

    // Reconnection uses sleep between attempts
    IQueueClientContextPtr SleepContext;
    TMaybe<TInstant> SessionExpireDeadline;
    TDuration SessionReconnectDelay = TDuration::Zero();

    IProcessor::TPtr Processor;
    THolder<TResponse> Response;

    ui64 SessionSeqNo = 0;
    ui64 SessionId = 0;
    ui64 NextReqId = 1;

    bool IsStopping = false;
};

////////////////////////////////////////////////////////////////////////////////

class TClient::TImpl : public TClientImplCommon<TClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncSessionResult StartSession(
        const TString& path,
        const TSessionSettings& settings)
    {
        auto session = MakeIntrusive<TSessionContext>(
            Connections_.get(),
            DbDriverState_,
            path,
            settings);

        auto result = session->TakeStartResult();

        session->Start(Connections_.get());

        return result;
    }

    TAsyncStatus CreateNode(
        Ydb::Coordination::CreateNodeRequest&& request,
        const TCreateNodeSettings& settings)
    {
        return RunSimple<Ydb::Coordination::V1::CoordinationService,
                        Ydb::Coordination::CreateNodeRequest,
                        Ydb::Coordination::CreateNodeResponse>(
            std::move(request),
            &Ydb::Coordination::V1::CoordinationService::Stub::AsyncCreateNode,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AlterNode(
        Ydb::Coordination::AlterNodeRequest&& request,
        const TAlterNodeSettings& settings)
    {
        return RunSimple<Ydb::Coordination::V1::CoordinationService,
                        Ydb::Coordination::AlterNodeRequest,
                        Ydb::Coordination::AlterNodeResponse>(
            std::move(request),
            &Ydb::Coordination::V1::CoordinationService::Stub::AsyncAlterNode,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus DropNode(
        Ydb::Coordination::DropNodeRequest&& request,
        const TDropNodeSettings& settings)
    {
        return RunSimple<Ydb::Coordination::V1::CoordinationService,
                        Ydb::Coordination::DropNodeRequest,
                        Ydb::Coordination::DropNodeResponse>(
            std::move(request),
            &Ydb::Coordination::V1::CoordinationService::Stub::AsyncDropNode,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribeNodeResult DescribeNode(
        Ydb::Coordination::DescribeNodeRequest&& request,
        const TDescribeNodeSettings& settings)
    {
        auto promise = NewPromise<TDescribeNodeResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Coordination::DescribeNodeResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                promise.SetValue(
                    TDescribeNodeResult(
                        TStatus(std::move(status)),
                        TNodeDescription(std::move(result))));
            };

        Connections_->RunDeferred<Ydb::Coordination::V1::CoordinationService,
                                Ydb::Coordination::DescribeNodeRequest,
                                Ydb::Coordination::DescribeNodeResponse>(
            std::move(request),
            std::move(extractor),
            &Ydb::Coordination::V1::CoordinationService::Stub::AsyncDescribeNode,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TClient::TClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{ }

TClient::~TClient() {
    // nothing special
}

TAsyncSessionResult TClient::StartSession(
    const TString& path,
    const TSessionSettings& settings)
{
    return Impl_->StartSession(path, settings);
}

TAsyncStatus TClient::CreateNode(
    const TString& path,
    const TCreateNodeSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Coordination::CreateNodeRequest>(settings);
    request.set_path(path);
    ConvertSettingsToProtoConfig(settings, request.mutable_config());
    return Impl_->CreateNode(std::move(request), settings);
}

TAsyncStatus TClient::AlterNode(
    const TString& path,
    const TAlterNodeSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Coordination::AlterNodeRequest>(settings);
    request.set_path(path);
    ConvertSettingsToProtoConfig(settings, request.mutable_config());
    return Impl_->AlterNode(std::move(request), settings);
}

TAsyncStatus TClient::DropNode(
    const TString& path,
    const TDropNodeSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Coordination::DropNodeRequest>(settings);
    request.set_path(path);
    return Impl_->DropNode(std::move(request), settings);
}

TAsyncDescribeNodeResult TClient::DescribeNode(
    const TString& path,
    const TDescribeNodeSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Coordination::DescribeNodeRequest>(settings);
    request.set_path(path);
    return Impl_->DescribeNode(std::move(request), settings);
}

////////////////////////////////////////////////////////////////////////////////

class TSession::TImpl {
public:
    TImpl(TSessionContext* context)
        : Context(context)
    { }

    ~TImpl() {
        Context->DoClose(true);
    }

    ui64 GetSessionId() {
        return Context->SessionId;
    }

    ESessionState GetSessionState() {
        return Context->DoGetSessionState();
    }

    EConnectionState GetConnectionState() {
        return Context->DoGetConnectionState();
    }

    TAsyncResult<void> Close() {
        return Context->DoClose(false);
    }

    TAsyncResult<void> Ping() {
        return Context->DoPing();
    }

    TAsyncResult<void> Reconnect() {
        return Context->DoReconnect();
    }

    TAsyncResult<bool> AcquireSemaphore(
            const TString& name,
            const TAcquireSemaphoreSettings& settings)
    {
        return Context->DoAcquireSemaphore(name, settings);
    }

    TAsyncResult<bool> ReleaseSemaphore(const TString& name) {
        return Context->DoReleaseSemaphore(name);
    }

    TAsyncDescribeSemaphoreResult DescribeSemaphore(
            const TString& name,
            const TDescribeSemaphoreSettings& settings)
    {
        return Context->DoDescribeSemaphore(name, settings);
    }

    TAsyncResult<void> CreateSemaphore(const TString& name, ui64 limit, const TString& data) {
        return Context->DoCreateSemaphore(name, limit, data);
    }

    TAsyncResult<void> UpdateSemaphore(const TString& name, const TString& data) {
        return Context->DoUpdateSemaphore(name, data);
    }

    TAsyncResult<void> DeleteSemaphore(const TString& name, bool force) {
        return Context->DoDeleteSemaphore(name, force);
    }

private:
    const TIntrusivePtr<TSessionContext> Context;
};

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(TSessionContext* context)
    : Impl_(std::make_shared<TImpl>(context))
{ }

ui64 TSession::GetSessionId() {
    return Impl_->GetSessionId();
}

ESessionState TSession::GetSessionState() {
    return Impl_->GetSessionState();
}

EConnectionState TSession::GetConnectionState() {
    return Impl_->GetConnectionState();
}

TAsyncResult<void> TSession::Close() {
    return Impl_->Close();
}

TAsyncResult<void> TSession::Ping() {
    return Impl_->Ping();
}

TAsyncResult<void> TSession::Reconnect() {
    return Impl_->Reconnect();
}

TAsyncResult<bool> TSession::AcquireSemaphore(
    const TString& name,
    const TAcquireSemaphoreSettings& settings)
{
    return Impl_->AcquireSemaphore(name, settings);
}

TAsyncResult<bool> TSession::ReleaseSemaphore(const TString& name) {
    return Impl_->ReleaseSemaphore(name);
}

TAsyncDescribeSemaphoreResult TSession::DescribeSemaphore(
    const TString& name,
    const TDescribeSemaphoreSettings& settings)
{
    return Impl_->DescribeSemaphore(name, settings);
}

TAsyncResult<void> TSession::CreateSemaphore(
    const TString& name,
    ui64 limit,
    const TString& data)
{
    return Impl_->CreateSemaphore(name, limit, data);
}

TAsyncResult<void> TSession::UpdateSemaphore(
    const TString& name,
    const TString& data)
{
    return Impl_->UpdateSemaphore(name, data);
}

TAsyncResult<void> TSession::DeleteSemaphore(
    const TString& name,
    bool force)
{
    return Impl_->DeleteSemaphore(name, force);
}

}
}
