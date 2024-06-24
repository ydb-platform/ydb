#include "kqp_workload_service_impl.h"
#include "kqp_workload_service_tables.h"

#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <ydb/library/actors/core/log.h>


namespace NKikimr::NKqp::NWorkload::NQueue {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadPool] " << LogPrefix() << stream)

using namespace NActors;


class TStateBase : public IState {
protected:
    struct TRequest {
        enum class EState {
            Pending,    // after TEvPlaceRequestIntoPool
            Finishing,  // after TEvCleanupRequest
            Canceling   // after TEvCancelRequest
        };

        TRequest(const TActorId& workerActorId, const TString& sessionId)
            : WorkerActorId(workerActorId)
            , SessionId(sessionId)
        {}

        const TActorId WorkerActorId;
        const TString SessionId;
        const TInstant StartTime = TInstant::Now();

        EState State = EState::Pending;
        bool Started = false;  // after TEvContinueRequest success
        bool CleanupRequired = false;
    };

public:
    TStateBase(const TActorContext& actorContext, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : Counters(counters)
        , ActorContext(actorContext)
        , PoolId(poolId)
        , QueueSizeLimit(GetMaxQueueSize(poolConfig))
        , InFlightLimit(GetMaxInFlight(poolConfig))
        , PoolConfig(poolConfig)
        , CancelAfter(poolConfig.QueryCancelAfter)
    {
        RegisterCounters();
    }

    ui64 GetLocalPoolSize() const final {
        return LocalSessions.size();
    }

    bool PlaceRequest(const TActorId& workerActorId, const TString& sessionId) final {
        if (LocalSessions.contains(sessionId)) {
            ActorContext.Send(workerActorId, new TEvContinueRequest(Ydb::StatusIds::INTERNAL_ERROR, PoolConfig, {NYql::TIssue(TStringBuilder() << "Got duplicate session id " << sessionId << " for pool " << PoolId)}));
            return false;
        }

        LOG_D("received new request, worker id: " << workerActorId << ", session id: " << sessionId);
        if (CancelAfter) {
            ActorContext.Schedule(CancelAfter, new TEvPrivate::TEvCancelRequest(PoolId, sessionId));
        }

        TRequest* request = &LocalSessions.insert({sessionId, TRequest(workerActorId, sessionId)}).first->second;
        LocalDelayedRequests->Inc();

        return OnScheduleRequest(request);
    }

    void CleanupRequest(const TActorId& workerActorId, const TString& sessionId) final {
        TRequest* request = GetRequestSafe(sessionId);
        if (!request || request->State == TRequest::EState::Canceling) {
            ActorContext.Send(workerActorId, new TEvCleanupResponse(Ydb::StatusIds::SUCCESS));
            return;
        }

        if (request->State == TRequest::EState::Finishing) {
            return;
        }
        request->State = TRequest::EState::Finishing;

        LOG_D("received cleanup request, worker id: " << workerActorId << ", session id: " << sessionId);
        OnCleanupRequest(request);
    }

    virtual void Handle(TEvPrivate::TEvCancelRequest::TPtr ev) override {
        TRequest* request = GetRequestSafe(ev->Get()->SessionId);
        if (!request || request->State == TRequest::EState::Finishing || request->State == TRequest::EState::Canceling) {
            return;
        }

        request->State = TRequest::EState::Canceling;

        LOG_D("cancel request by deadline, worker id: " << request->WorkerActorId << ", session id: " << request->SessionId);
        OnCleanupRequest(request);
    }

public:
    void ReplyContinue(TRequest* request, Ydb::StatusIds::StatusCode status, const TString& message) {
        ReplyContinue(request, status, {NYql::TIssue(message)});
    }

    void ReplyContinue(TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, NYql::TIssues issues = {}) {
        ActorContext.Send(request->WorkerActorId, new TEvContinueRequest(status, PoolConfig, std::move(issues)));

        if (status == Ydb::StatusIds::SUCCESS) {
            LocalInFlight++;
            request->Started = true;
            LocalInFly->Inc();
            ContinueOk->Inc();
            DelayedTimeMs->Collect((TInstant::Now() - request->StartTime).MilliSeconds());
            LOG_D("Reply continue success to " << request->WorkerActorId << ", session id: " << request->SessionId << ", local in flight: " << LocalInFlight);
        } else {
            if (status == Ydb::StatusIds::OVERLOADED) {
                ContinueOverloaded->Inc();
                LOG_I("Reply overloaded to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
            } else if (status == Ydb::StatusIds::CANCELLED) {
                Cancelled->Inc();
                LOG_I("Reply cancelled to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
            } else {
                ContinueError->Inc();
                LOG_W("Reply continue error " << status << " to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
            }
            LocalSessions.erase(request->SessionId);
        }

        LocalDelayedRequests->Dec();
    }

    void FinalReply(TRequest* request, Ydb::StatusIds::StatusCode status, const TString& message) {
        FinalReply(request, status, {NYql::TIssue(message)});
    }

    void FinalReply(TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, NYql::TIssues issues = {}) {
        if (!request->Started && request->State != TRequest::EState::Finishing) {
            if (request->State == TRequest::EState::Canceling && status == Ydb::StatusIds::SUCCESS) {
                status = Ydb::StatusIds::CANCELLED;
                issues.AddIssue(TStringBuilder() << "Delay deadline exceeded in pool " << PoolId);
            }
            ReplyContinue(request, status, issues);
            return;
        }

        if (request->Started) {
            LocalInFlight--;
            LocalInFly->Dec();
        } else {
            LocalDelayedRequests->Dec();
        }

        if (request->State == TRequest::EState::Canceling) {
            ReplyCancel(request);
        } else {
            ReplyCleanup(request, status, std::move(issues));
        }

        LocalSessions.erase(request->SessionId);
    }

protected:
    virtual bool OnScheduleRequest(TRequest* request) = 0;
    virtual void OnCleanupRequest(TRequest* request) = 0;

protected:
    TRequest* GetRequest(const TString& sessionId) {
        auto resultIt = LocalSessions.find(sessionId);
        Y_ENSURE(resultIt != LocalSessions.end(), "Invalid session id " << sessionId);
        return &resultIt->second;
    }

    TRequest* GetRequestSafe(const TString& sessionId) {
        auto resultIt = LocalSessions.find(sessionId);
        if (resultIt != LocalSessions.end()) {
            return &resultIt->second;
        }
        return nullptr;
    }

    ui64 GetLocalInFlight() const {
        return LocalInFlight;
    }

    TMaybe<TInstant> GetWaitDeadline(TInstant startTime) {
        if (!CancelAfter) {
            return Nothing();
        }
        return startTime + CancelAfter;
    }

    NYql::TIssue GroupIssues(const TString& message, NYql::TIssues issues) {
        NYql::TIssue rootIssue(message);
        for (const NYql::TIssue& issue : issues) {
            rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
        }
        return rootIssue;
    }

    TString LogPrefix() const {
        return TStringBuilder() << "PoolId: " << PoolId << ", ";
    }

private:
    void ReplyCleanup(const TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, NYql::TIssues issues = {}) {
        ActorContext.Send(request->WorkerActorId, new TEvCleanupResponse(status, std::move(issues)));

        if (status == Ydb::StatusIds::SUCCESS) {
            CleanupOk->Inc();
            RequestsLatencyMs->Collect((TInstant::Now() - request->StartTime).MilliSeconds());
            LOG_D("Reply cleanup success to " << request->WorkerActorId << ", session id: " << request->SessionId << ", local in flight: " << LocalInFlight);
        } else {
            CleanupError->Inc();
            LOG_W("Reply cleanup error " << status << " to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
        }
    }

    void ReplyCancel(const TRequest* request) {
        auto ev = std::make_unique<TEvKqp::TEvCancelQueryRequest>();
        ev->Record.MutableRequest()->SetSessionId(request->SessionId);
        ActorContext.Send(MakeKqpProxyID(ActorContext.SelfID.NodeId()), ev.release());

        Cancelled->Inc();
        RequestsLatencyMs->Collect((TInstant::Now() - request->StartTime).MilliSeconds());
        LOG_I("Cancel request for worker " << request->WorkerActorId << ", session id: " << request->SessionId << ", local in flight: " << LocalInFlight);
    }

    static ui64 GetMaxQueueSize(const NResourcePool::TPoolSettings& poolConfig) {
        const auto queueSize = poolConfig.QueueSize;
        return queueSize == -1 ? std::numeric_limits<ui64>::max() : static_cast<ui64>(queueSize);
    }

    static ui64 GetMaxInFlight(const NResourcePool::TPoolSettings& poolConfig) {
        const auto concurrentQueryLimit = poolConfig.ConcurrentQueryLimit;
        return concurrentQueryLimit == -1 ? std::numeric_limits<ui64>::max() : static_cast<ui64>(concurrentQueryLimit);
    }

    void RegisterCounters() {
        LocalInFly = Counters->GetCounter("LocalInFly", false);
        LocalDelayedRequests = Counters->GetCounter("LocalDelayedRequests", false);
        ContinueOk = Counters->GetCounter("ContinueOk", true);
        ContinueOverloaded = Counters->GetCounter("ContinueOverloaded", true);
        ContinueError = Counters->GetCounter("ContinueError", true);
        CleanupOk = Counters->GetCounter("CleanupOk", true);
        CleanupError = Counters->GetCounter("CleanupError", true);
        Cancelled = Counters->GetCounter("Cancelled", true);
        DelayedTimeMs = Counters->GetHistogram("DelayedTimeMs", NMonitoring::ExponentialHistogram(20, 2, 4));
        RequestsLatencyMs = Counters->GetHistogram("RequestsLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 4));
    }

protected:
    NMonitoring::TDynamicCounterPtr Counters;

    const TActorContext ActorContext;
    const TString PoolId;
    const ui64 QueueSizeLimit;
    const ui64 InFlightLimit;

private:
    const NResourcePool::TPoolSettings PoolConfig;
    const TDuration CancelAfter;

    ui64 LocalInFlight = 0;
    std::unordered_map<TString, TRequest> LocalSessions;

    NMonitoring::TDynamicCounters::TCounterPtr LocalInFly;
    NMonitoring::TDynamicCounters::TCounterPtr LocalDelayedRequests;
    NMonitoring::TDynamicCounters::TCounterPtr ContinueOk;
    NMonitoring::TDynamicCounters::TCounterPtr ContinueOverloaded;
    NMonitoring::TDynamicCounters::TCounterPtr ContinueError;
    NMonitoring::TDynamicCounters::TCounterPtr CleanupOk;
    NMonitoring::TDynamicCounters::TCounterPtr CleanupError;
    NMonitoring::TDynamicCounters::TCounterPtr Cancelled;
    NMonitoring::THistogramPtr DelayedTimeMs;
    NMonitoring::THistogramPtr RequestsLatencyMs;
};


class TUnlimitedState : public TStateBase {
    using TBase = TStateBase;

public:
    TUnlimitedState(const TActorContext& actorContext, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(actorContext, poolId, poolConfig, counters)
    {
        Y_ENSURE(InFlightLimit == std::numeric_limits<ui64>::max());
    }

    bool TablesRequired() const override {
        return false;
    }

    void OnPreparingFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) override {
        Y_UNUSED(status, issues);
    }

    void RefreshState(bool refreshRequired = false) override {
        Y_UNUSED(refreshRequired);
    }

    bool OnScheduleRequest(TRequest* request) override {
        ReplyContinue(request);
        return true;
    }

    void OnCleanupRequest(TRequest* request) override {
        FinalReply(request);
    }
};


class TFifoState : public TStateBase {
    using TBase = TStateBase;

    static constexpr ui64 MAX_PENDING_REQUESTS = 1000;

public:
    TFifoState(TActorContext actorContext, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(actorContext, poolId, poolConfig, counters)
    {
        Y_ENSURE(InFlightLimit < std::numeric_limits<ui64>::max());
        RegisterCounters();
    }

    bool TablesRequired() const override {
        return true;
    }

    void OnPreparingFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            PreparingFinished = true;
            RefreshState();
            return;
        }

        ForEachUnfinished(PendingRequests.begin(), PendingRequests.end(), [this, status, issues](TRequest* request) {
            ReplyContinue(request, status, issues);
        });

        PendingRequests.clear();
        PendingRequestsCount->Set(0);
    }

    void RefreshState(bool refreshRequired = false) override {
        RefreshRequired |= refreshRequired;
        if (!PreparingFinished) {
            return;
        }

        DoCleanupRequests();
        if (RunningOperation) {
            return;
        }

        RefreshRequired |= !PendingRequests.empty();
        RefreshRequired |= (GetLocalInFlight() || !DelayedRequests.empty()) && TInstant::Now() - LastRefreshTime > LEASE_DURATION / 4;
        if (RefreshRequired) {
            RefreshRequired = false;
            RunningOperation = true;
            ActorContext.Register(CreateRefreshPoolStateActor(ActorContext.SelfID, PoolId, LEASE_DURATION, Counters));
        }
    }

    bool OnScheduleRequest(TRequest* request) override {
        if (PendingRequests.size() >= MAX_PENDING_REQUESTS || PendingRequests.size() + DelayedRequests.size() > QueueSizeLimit) {
            ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
            return false;
        }

        PendingRequests.emplace_back(request->SessionId);
        PendingRequestsCount->Inc();

        RefreshState();
        return true;
    }

    void OnCleanupRequest(TRequest* request) override {
        if (!request->CleanupRequired) {
            FinalReply(request);
        } else {
            AddFinishedRequest(request->SessionId);
        }
        RefreshState();
    }

    void Handle(TEvPrivate::TEvCancelRequest::TPtr ev) override {
        TBase::Handle(std::move(ev));
    };

    void Handle(TEvPrivate::TEvRefreshPoolStateResponse::TPtr ev) override {
        RunningOperation = false;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("refresh pool state failed " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            RefreshRequired = true;
            return;
        }

        if (LastRefreshTime) {
            PoolStateUpdatesBacklogMs->Collect((TInstant::Now() - LastRefreshTime).MilliSeconds());
        }
        LastRefreshTime = TInstant::Now();

        GlobalState = ev->Get()->PoolState;
        GlobalInFly->Set(GlobalState.RunningRequests);
        GlobalDelayedRequests->Set(GlobalState.DelayedRequests);
        LOG_T("succefully refreshed pool state, in flight: " << GlobalState.RunningRequests << ", delayed: " << GlobalState.DelayedRequests);

        RemoveFinishedRequests();
        DoStartPendingRequest();

        if (GlobalState.DelayedRequests + PendingRequests.size() > QueueSizeLimit) {
            ui64 countToDelete = std::min(GlobalState.DelayedRequests + PendingRequests.size() - QueueSizeLimit, PendingRequests.size());
            auto firstRequest = PendingRequests.begin() + (PendingRequests.size() - countToDelete);
            ForEachUnfinished(firstRequest, PendingRequests.end(), [this](TRequest* request) {
                ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
            });
            PendingRequests.erase(firstRequest, PendingRequests.end());
            PendingRequestsCount->Set(PendingRequests.size());
        }

        DoDelayRequest();
        DoStartDelayedRequest();
    };

    void Handle(TEvPrivate::TEvDelayRequestResponse::TPtr ev) override {
        RunningOperation = false;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("failed to delay request " << ev->Get()->Status << ", session id: " << ev->Get()->SessionId << ", issues: " << ev->Get()->Issues.ToOneLineString());
            NYql::TIssue issue = GroupIssues("Failed to put request in queue", ev->Get()->Issues);
            ForUnfinished(ev->Get()->SessionId, [this, ev, issue](TRequest* request) {
                ReplyContinue(request, ev->Get()->Status, {issue});
            });
            RefreshRequired = true;
            return;
        }

        GlobalState.DelayedRequests++;
        GlobalDelayedRequests->Inc();
        LOG_D("succefully delayed request, session id: " << ev->Get()->SessionId);

        DoStartDelayedRequest();
        RefreshState();
    };

    void Handle(TEvPrivate::TEvStartRequestResponse::TPtr ev) override {
        RunningOperation = false;

        const TString& sessionId = ev->Get()->SessionId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("failed start request " << ev->Get()->Status << ", session id: " << sessionId << ", issues: " << ev->Get()->Issues.ToOneLineString());
            NYql::TIssue issue = GroupIssues("Failed to start request", ev->Get()->Issues);
            ForUnfinished(sessionId, [this, ev, issue](TRequest* request) {
                AddFinishedRequest(request->SessionId);
                ReplyContinue(request, ev->Get()->Status, {issue});
            });
            RefreshState();
            return;
        }

        if (!sessionId) {
            LOG_D("first request in queue is remote, send notification to node " << ev->Get()->NodeId);
            auto event = std::make_unique<TEvPrivate::TEvRefreshPoolState>();
            event->Record.SetPoolId(PoolId);
            ActorContext.Send(MakeKqpWorkloadServiceId(ev->Get()->NodeId), std::move(event));
            RefreshState();
            return;
        }

        LOG_D("started request, session id: " << sessionId);
        bool requestFound = false;
        while (!DelayedRequests.empty() && !requestFound) {
            ForUnfinished(DelayedRequests.front(), [this, sessionId, &requestFound](TRequest* request) {
                if (request->SessionId == sessionId) {
                    requestFound = true;
                    GlobalState.RunningRequests++;
                    GlobalInFly->Inc();
                    ReplyContinue(request);
                } else {
                    // Request was dropped due to lease expiration 
                    PendingRequests.emplace_front(request->SessionId);
                    PendingRequestsCount->Inc();
                }
            });
            DelayedRequests.pop_front();
        }
        if (!requestFound) {
            AddFinishedRequest(sessionId);
        }
        RefreshState(!DelayedRequests.empty() && GlobalState.RunningRequests < InFlightLimit);
    };

    void Handle(TEvPrivate::TEvCleanupRequestsResponse::TPtr ev) override {
        RunningOperation = false;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("cleanup requests failed " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        }

        for (const TString& sessionId : ev->Get()->SesssionIds) {
            LOG_T("succefully cleanuped request, session id: " << sessionId);
            if (TRequest* request = GetRequestSafe(sessionId)) {
                FinalReply(request, ev->Get()->Status, ev->Get()->Issues);
            }
        }

        RefreshState(true);
    };

private:
    void DoStartPendingRequest() {
        RemoveFinishedRequests();
        if (RunningOperation) {
            return;
        }

        if (!PendingRequests.empty() && DelayedRequests.empty() && GlobalState.DelayedRequests == 0 && GlobalState.RunningRequests < InFlightLimit) {
            RunningOperation = true;
            const TString& sessionId = PopPendingRequest();
            ActorContext.Register(CreateStartRequestActor(PoolId, sessionId, LEASE_DURATION, Counters));
            DelayedRequests.emplace_front(sessionId);
            GetRequest(sessionId)->CleanupRequired = true;
        }
    }

    void DoStartDelayedRequest() {
        RemoveFinishedRequests();
        if (RunningOperation) {
            return;
        }

        if ((!DelayedRequests.empty() || GlobalState.DelayedRequests) && GlobalState.RunningRequests < InFlightLimit) {
            RunningOperation = true;
            ActorContext.Register(CreateStartRequestActor(PoolId, std::nullopt, LEASE_DURATION, Counters));
        }
    }

    void DoDelayRequest() {
        RemoveFinishedRequests();
        if (RunningOperation) {
            return;
        }

        if (!PendingRequests.empty()) {
            RunningOperation = true;
            const TString& sessionId = PopPendingRequest();
            TRequest* request = GetRequest(sessionId);
            ActorContext.Register(CreateDelayRequestActor(ActorContext.SelfID, PoolId, sessionId, request->StartTime, GetWaitDeadline(request->StartTime), LEASE_DURATION, Counters));
            DelayedRequests.emplace_back(sessionId);
            request->CleanupRequired = true;
        }
    }

    void DoCleanupRequests() {
        if (RunningOperation) {
            return;
        }

        if (!FinishedRequests.empty()) {
            RunningOperation = true;
            ActorContext.Register(CreateCleanupRequestsActor(ActorContext.SelfID, PoolId, FinishedRequests, Counters));
            FinishedRequests.clear();
            FinishingRequestsCount->Set(0);
        }
    }

private:
    void RemoveFinishedRequests() {
        if (RunningOperation) {
            return;
        }

        RemoveFinishedRequests(PendingRequests);
        RemoveFinishedRequests(DelayedRequests);
        PendingRequestsCount->Set(PendingRequests.size());
    }

    void RemoveFinishedRequests(std::deque<TString>& requests) {
        while (!requests.empty()) {
            TRequest* request = GetRequestSafe(requests.front());
            if (!RequestFinished(request)) {
                break;
            }
            requests.pop_front();
        }
    }

    template <typename TIterator>
    void ForEachUnfinished(TIterator begin, TIterator end, std::function<void(TRequest*)> func) {
        for (; begin != end; ++begin) {
            ForUnfinished(*begin, func);
        }
    }

    void ForUnfinished(const TString& sessionId, std::function<void(TRequest*)> func) {
        TRequest* request = GetRequestSafe(sessionId);
        if (!RequestFinished(request)) {
            func(request);
        }
    }

    static bool RequestFinished(const TRequest* request) {
        return !request || request->State == TRequest::EState::Finishing || request->State == TRequest::EState::Canceling;
    }

private:
    TString PopPendingRequest() {
        TString sessionId = PendingRequests.front();
        PendingRequests.pop_front();
        PendingRequestsCount->Dec();
        return sessionId;
    }

    void AddFinishedRequest(const TString& sessionId) {
        FinishedRequests.emplace_back(sessionId);
        FinishingRequestsCount->Inc();
    }

    void RegisterCounters() {
        PendingRequestsCount = Counters->GetCounter("PendingRequestsCount", false);
        FinishingRequestsCount = Counters->GetCounter("FinishingRequestsCount", false);
        GlobalInFly = Counters->GetCounter("GlobalInFly", false);
        GlobalDelayedRequests = Counters->GetCounter("GlobalDelayedRequests", false);
        PoolStateUpdatesBacklogMs = Counters->GetHistogram("PoolStateUpdatesBacklogMs", NMonitoring::LinearHistogram(20, 0, 3 * LEASE_DURATION.MillisecondsFloat() / 40));
    }

private:
    bool PreparingFinished = false;
    bool RefreshRequired = false;
    bool RunningOperation = false;

    std::deque<TString> PendingRequests;
    std::deque<TString> DelayedRequests;
    std::vector<TString> FinishedRequests;

    TInstant LastRefreshTime = TInstant::Zero();
    TPoolStateDescription GlobalState;

    NMonitoring::TDynamicCounters::TCounterPtr PendingRequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr FinishingRequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr GlobalInFly;
    NMonitoring::TDynamicCounters::TCounterPtr GlobalDelayedRequests;
    NMonitoring::THistogramPtr PoolStateUpdatesBacklogMs;
};

}  // anonymous namespace

TStatePtr CreateState(const TActorContext& actorContext, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters) {
    if (poolConfig.ConcurrentQueryLimit == -1) {
        return MakeIntrusive<TUnlimitedState>(actorContext, poolId, poolConfig, counters);
    }
    return MakeIntrusive<TFifoState>(actorContext, poolId, poolConfig, counters);
}

}  // NKikimr::NKqp::NWorkload::NQueue
