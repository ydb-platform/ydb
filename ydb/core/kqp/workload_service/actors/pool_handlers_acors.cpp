#include "actors.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/kqp/workload_service/tables/table_queries.h>


namespace NKikimr::NKqp::NWorkload {

namespace {

using namespace NActors;

constexpr TDuration LEASE_DURATION = TDuration::Seconds(30);


template <typename TDerived>
class TPoolHandlerActorBase : public TActor<TDerived> {
    using TBase = TActor<TDerived>;

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
    TPoolHandlerActorBase(void (TDerived::* requestFunc)(TAutoPtr<IEventHandle>& ev), const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(requestFunc)
        , Counters(counters)
        , Database(database)
        , PoolId(poolId)
        , QueueSizeLimit(GetMaxQueueSize(poolConfig))
        , InFlightLimit(GetMaxInFlight(poolConfig))
        , PoolConfig(poolConfig)
        , CancelAfter(poolConfig.QueryCancelAfter)
    {
        RegisterCounters();
    }

    STRICT_STFUNC(StateFuncBase,
        sFunc(TEvents::TEvPoison, HandlePoison);

        hFunc(TEvPlaceRequestIntoPool, Handle);
        hFunc(TEvCleanupRequest, Handle);
        hFunc(TEvPrivate::TEvCancelRequest, Handle);

        IgnoreFunc(TEvKqp::TEvCancelQueryResponse);
    )

private:
    void HandlePoison() {
        LOG_W("Got poison, stop pool handler");
        this->PassAway();
    }

    void Handle(TEvPlaceRequestIntoPool::TPtr& ev) {
        const TActorId& workerActorId = ev->Sender;
        if (!InFlightLimit) {
            this->Send(workerActorId, new TEvContinueRequest(Ydb::StatusIds::PRECONDITION_FAILED, PoolId, PoolConfig, {NYql::TIssue(TStringBuilder() << "Resource pool " << PoolId << " was disabled due to zero concurrent query limit")}));
            return;
        }

        const TString& sessionId = ev->Get()->SessionId;
        if (LocalSessions.contains(sessionId)) {
            this->Send(workerActorId, new TEvContinueRequest(Ydb::StatusIds::INTERNAL_ERROR, PoolId, PoolConfig, {NYql::TIssue(TStringBuilder() << "Got duplicate session id " << sessionId << " for pool " << PoolId)}));
            return;
        }

        LOG_D("Received new request, worker id: " << workerActorId << ", session id: " << sessionId);
        if (CancelAfter) {
            this->Schedule(CancelAfter, new TEvPrivate::TEvCancelRequest(sessionId));
        }

        TRequest* request = &LocalSessions.insert({sessionId, TRequest(workerActorId, sessionId)}).first->second;
        LocalDelayedRequests->Inc();

        OnScheduleRequest(request);
    }

    void Handle(TEvCleanupRequest::TPtr& ev) {
        const TString& sessionId = ev->Get()->SessionId;
        const TActorId& workerActorId = ev->Sender;

        TRequest* request = GetRequestSafe(sessionId);
        if (!request || request->State == TRequest::EState::Canceling) {
            this->Send(workerActorId, new TEvCleanupResponse(Ydb::StatusIds::SUCCESS));
            return;
        }

        if (request->State == TRequest::EState::Finishing) {
            return;
        }
        request->State = TRequest::EState::Finishing;

        LOG_D("Received cleanup request, worker id: " << workerActorId << ", session id: " << sessionId);
        OnCleanupRequest(request);
    }

    void Handle(TEvPrivate::TEvCancelRequest::TPtr& ev) {
        TRequest* request = GetRequestSafe(ev->Get()->SessionId);
        if (!request || request->State == TRequest::EState::Finishing || request->State == TRequest::EState::Canceling) {
            return;
        }

        request->State = TRequest::EState::Canceling;

        LOG_D("Cancel request by deadline, worker id: " << request->WorkerActorId << ", session id: " << request->SessionId);
        OnCleanupRequest(request);
    }

public:
    void ReplyContinue(TRequest* request, Ydb::StatusIds::StatusCode status, const TString& message) {
        ReplyContinue(request, status, {NYql::TIssue(message)});
    }

    void ReplyContinue(TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, NYql::TIssues issues = {}) {
        this->Send(request->WorkerActorId, new TEvContinueRequest(status, PoolId, PoolConfig, std::move(issues)));

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
    virtual void OnScheduleRequest(TRequest* request) = 0;
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

    TMaybe<TInstant> GetWaitDeadline(TInstant startTime) const {
        if (!CancelAfter) {
            return Nothing();
        }
        return startTime + CancelAfter;
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TPoolHandlerActorBase] ActorId: " << this->SelfId() << ", Database: " << Database << ", PoolId: " << PoolId << ", ";
    }

private:
    void ReplyCleanup(const TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, NYql::TIssues issues = {}) {
        this->Send(request->WorkerActorId, new TEvCleanupResponse(status, std::move(issues)));

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
        this->Send(MakeKqpProxyID(this->SelfId().NodeId()), ev.release());

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

    const TString Database;
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


class TUnlimitedPoolHandlerActor : public TPoolHandlerActorBase<TUnlimitedPoolHandlerActor> {
    using TBase = TPoolHandlerActorBase<TUnlimitedPoolHandlerActor>;

public:
    TUnlimitedPoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TBase::StateFuncBase, database, poolId, poolConfig, counters)
    {
        Y_ENSURE(InFlightLimit == std::numeric_limits<ui64>::max() || !InFlightLimit);
    }

protected:
    void OnScheduleRequest(TRequest* request) override {
        ReplyContinue(request);
    }

    void OnCleanupRequest(TRequest* request) override {
        FinalReply(request);
    }
};


class TFifoPoolHandlerActor : public TPoolHandlerActorBase<TFifoPoolHandlerActor> {
    using TBase = TPoolHandlerActorBase<TFifoPoolHandlerActor>;

    static constexpr ui64 MAX_PENDING_REQUESTS = 1000;

public:
    TFifoPoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TFifoPoolHandlerActor::StateFunc, database, poolId, poolConfig, counters)
    {
        Y_ENSURE(InFlightLimit < std::numeric_limits<ui64>::max());
        RegisterCounters();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, HandleRefreshState);
            sFunc(TEvPrivate::TEvRefreshPoolState, HandleExternalRefreshState);

            hFunc(TEvPrivate::TEvTablesCreationFinished, Handle);
            hFunc(TEvPrivate::TEvRefreshPoolStateResponse, Handle);
            hFunc(TEvPrivate::TEvDelayRequestResponse, Handle);
            hFunc(TEvPrivate::TEvStartRequestResponse, Handle);
            hFunc(TEvPrivate::TEvCleanupRequestsResponse, Handle);
            default:
                StateFuncBase(ev);
        }
    }

protected:
    void OnScheduleRequest(TRequest* request) override {
        if (PendingRequests.size() >= MAX_PENDING_REQUESTS || PendingRequests.size() + DelayedRequests.size() > QueueSizeLimit) {
            ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
            return;
        }

        PendingRequests.emplace_back(request->SessionId);
        PendingRequestsCount->Inc();

        if (!PreparingFinished) {
            this->Send(MakeKqpWorkloadServiceId(this->SelfId().NodeId()), new TEvPrivate::TEvPrepareTablesRequest(Database, PoolId));
        }

        RefreshState();
    }

    void OnCleanupRequest(TRequest* request) override {
        if (!request->CleanupRequired) {
            FinalReply(request);
        } else {
            AddFinishedRequest(request->SessionId);
        }
        RefreshState();
    }

    void RefreshState(bool refreshRequired = false) {
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
            this->Register(CreateRefreshPoolStateActor(this->SelfId(), Database, PoolId, LEASE_DURATION, Counters));
        }
    }

private:
    void HandleRefreshState() {
        RefreshScheduled = false;
        LOG_T("Try to start scheduled refresh");

        RefreshState();
        if (GetLocalInFlight() + DelayedRequests.size() > 0) {
            ScheduleRefresh();
        }
    }

    void HandleExternalRefreshState() {
        LOG_D("Got remote refresh request");
        RefreshState(true);
    }

    void Handle(TEvPrivate::TEvTablesCreationFinished::TPtr& ev) {
        if (ev->Get()->Success) {
            PreparingFinished = true;
            RefreshState();
            return;
        }

        NYql::TIssues issues = GroupIssues(ev->Get()->Issues, "Failed to create workload service tables");
        ForEachUnfinished(PendingRequests.begin(), PendingRequests.end(), [this, issues](TRequest* request) {
            ReplyContinue(request, Ydb::StatusIds::INTERNAL_ERROR, issues);
        });

        PendingRequests.clear();
        PendingRequestsCount->Set(0);
    }

    void Handle(TEvPrivate::TEvRefreshPoolStateResponse::TPtr& ev) {
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

    void Handle(TEvPrivate::TEvDelayRequestResponse::TPtr& ev) {
        RunningOperation = false;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("failed to delay request " << ev->Get()->Status << ", session id: " << ev->Get()->SessionId << ", issues: " << ev->Get()->Issues.ToOneLineString());
            NYql::TIssues issues = GroupIssues(ev->Get()->Issues, "Failed to put request in queue");
            ForUnfinished(ev->Get()->SessionId, [this, ev, issues](TRequest* request) {
                ReplyContinue(request, ev->Get()->Status, issues);
            });
            RefreshRequired = true;
            return;
        }

        GlobalState.DelayedRequests++;
        GlobalDelayedRequests->Inc();
        LOG_D("succefully delayed request, session id: " << ev->Get()->SessionId);

        ScheduleRefresh();
        DoStartDelayedRequest();
        RefreshState();
    };

    void Handle(TEvPrivate::TEvStartRequestResponse::TPtr& ev) {
        RunningOperation = false;

        const TString& sessionId = ev->Get()->SessionId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("failed start request " << ev->Get()->Status << ", session id: " << sessionId << ", issues: " << ev->Get()->Issues.ToOneLineString());
            NYql::TIssues issues = GroupIssues(ev->Get()->Issues, "Failed to start request");
            ForUnfinished(sessionId, [this, ev, issues](TRequest* request) {
                AddFinishedRequest(request->SessionId);
                ReplyContinue(request, ev->Get()->Status, issues);
            });
            RefreshState();
            return;
        }

        if (!sessionId) {
            LOG_D("first request in queue is remote, send notification to node " << ev->Get()->NodeId);
            auto event = std::make_unique<TEvPrivate::TEvRefreshPoolState>();
            event->Record.SetPoolId(PoolId);
            event->Record.SetDatabase(Database);
            this->Send(MakeKqpWorkloadServiceId(ev->Get()->NodeId), std::move(event));
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

        ScheduleRefresh();
        RefreshState(!DelayedRequests.empty() && GlobalState.RunningRequests < InFlightLimit);
    };

    void Handle(TEvPrivate::TEvCleanupRequestsResponse::TPtr& ev) {
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
            this->Register(CreateStartRequestActor(this->SelfId(), Database, PoolId, sessionId, LEASE_DURATION, Counters));
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
            this->Register(CreateStartRequestActor(this->SelfId(), Database, PoolId, std::nullopt, LEASE_DURATION, Counters));
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
            this->Register(CreateDelayRequestActor(this->SelfId(), Database, PoolId, sessionId, request->StartTime, GetWaitDeadline(request->StartTime), LEASE_DURATION, Counters));
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
            this->Register(CreateCleanupRequestsActor(this->SelfId(), Database, PoolId, FinishedRequests, Counters));
            FinishedRequests.clear();
            FinishingRequestsCount->Set(0);
        }
    }

    void ScheduleRefresh() {
        if (RefreshScheduled) {
            return;
        }
        RefreshScheduled = true;
        this->Schedule(LEASE_DURATION / 2, new TEvents::TEvWakeup());
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
    bool RefreshScheduled = false;

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

IActor* CreatePoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters) {
    if (poolConfig.ConcurrentQueryLimit <= 0) {
        return new TUnlimitedPoolHandlerActor(database, poolId, poolConfig, counters);
    }
    return new TFifoPoolHandlerActor(database, poolId, poolConfig, counters);
}

}  // NKikimr::NKqp::NWorkload
