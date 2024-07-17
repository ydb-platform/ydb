#include "actors.h"

#include <ydb/core/base/path.h>

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

    struct TCommonCounters {
        const NMonitoring::TDynamicCounterPtr CountersRoot;
        const NMonitoring::TDynamicCounterPtr CountersSubgroup;

        // Workload service counters
        NMonitoring::TDynamicCounters::TCounterPtr ActivePoolHandlers;

        // Pool counters
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

        // Config counters
        NMonitoring::TDynamicCounters::TCounterPtr InFlightLimit;
        NMonitoring::TDynamicCounters::TCounterPtr QueueSizeLimit;
        NMonitoring::TDynamicCounters::TCounterPtr LoadCpuThreshold;

        TCommonCounters(NMonitoring::TDynamicCounterPtr counters, const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig)
            : CountersRoot(counters)
            , CountersSubgroup(counters->GetSubgroup("pool", CanonizePath(TStringBuilder() << database << "/" << poolId)))
        {
            Register();
            UpdateConfigCounters(poolConfig);
        }

        void UpdateConfigCounters(const NResourcePool::TPoolSettings& poolConfig) {
            InFlightLimit->Set(std::max(poolConfig.ConcurrentQueryLimit, 0));
            QueueSizeLimit->Set(std::max(poolConfig.QueueSize, 0));
            LoadCpuThreshold->Set(std::max(poolConfig.DatabaseLoadCpuThreshold, 0.0));
        }

        void OnCleanup() {
            ActivePoolHandlers->Dec();

            InFlightLimit->Set(0);
            QueueSizeLimit->Set(0);
            LoadCpuThreshold->Set(0);
        }

    private:
        void Register() {
            ActivePoolHandlers = CountersRoot->GetCounter("ActivePoolHandlers", false);
            ActivePoolHandlers->Inc();

            LocalInFly = CountersSubgroup->GetCounter("LocalInFly", false);
            LocalDelayedRequests = CountersSubgroup->GetCounter("LocalDelayedRequests", false);
            ContinueOk = CountersSubgroup->GetCounter("ContinueOk", true);
            ContinueOverloaded = CountersSubgroup->GetCounter("ContinueOverloaded", true);
            ContinueError = CountersSubgroup->GetCounter("ContinueError", true);
            CleanupOk = CountersSubgroup->GetCounter("CleanupOk", true);
            CleanupError = CountersSubgroup->GetCounter("CleanupError", true);
            Cancelled = CountersSubgroup->GetCounter("Cancelled", true);
            DelayedTimeMs = CountersSubgroup->GetHistogram("DelayedTimeMs", NMonitoring::ExponentialHistogram(20, 2, 4));
            RequestsLatencyMs = CountersSubgroup->GetHistogram("RequestsLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 4));

            InFlightLimit = CountersSubgroup->GetCounter("InFlightLimit", false);
            QueueSizeLimit = CountersSubgroup->GetCounter("QueueSizeLimit", false);
            LoadCpuThreshold = CountersSubgroup->GetCounter("LoadCpuThreshold", false);
        }
    };

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
        bool UsedCpuQuota = false;
        TDuration Duration;
        TDuration CpuConsumed;
    };

public:
    TPoolHandlerActorBase(void (TDerived::* requestFunc)(TAutoPtr<IEventHandle>& ev), const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(requestFunc)
        , Counters(counters, database, poolId, poolConfig)
        , Database(database)
        , PoolId(poolId)
        , QueueSizeLimit(GetMaxQueueSize(poolConfig))
        , InFlightLimit(GetMaxInFlight(poolConfig))
        , PoolConfig(poolConfig)
    {}

    STRICT_STFUNC(StateFuncBase,
        // Workload service events
        sFunc(TEvents::TEvPoison, HandlePoison);
        sFunc(TEvPrivate::TEvStopPoolHandler, HandleStop);
        hFunc(TEvPrivate::TEvResolvePoolResponse, Handle);

        // Pool handler events
        hFunc(TEvPrivate::TEvCancelRequest, Handle);

        // Worker actor events
        hFunc(TEvCleanupRequest, Handle);
        IgnoreFunc(TEvKqp::TEvCancelQueryResponse);

        // Schemeboard events
        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
    )

    virtual void PassAway() override {
        if (WatchPathId) {
            this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(0));
        }

        Counters.OnCleanup();

        TBase::PassAway();
    }

private:
    void HandlePoison() {
        LOG_W("Got poison, stop pool handler");
        this->PassAway();
    }

    void HandleStop() {
        LOG_I("Got stop pool handler request, waiting for " << LocalSessions.size() << " requests");
        if (LocalSessions.empty()) {
            PassAway();
        } else {
            StopHandler = true;
        }
    }

    void Handle(TEvPrivate::TEvResolvePoolResponse::TPtr& ev) {
        auto event = std::move(ev->Get()->Event);
        const TActorId& workerActorId = event->Sender;
        if (!InFlightLimit) {
            this->Send(workerActorId, new TEvContinueRequest(Ydb::StatusIds::PRECONDITION_FAILED, PoolId, PoolConfig, {NYql::TIssue(TStringBuilder() << "Resource pool " << PoolId << " was disabled due to zero concurrent query limit")}));
            return;
        }

        const TString& sessionId = event->Get()->SessionId;
        if (LocalSessions.contains(sessionId)) {
            this->Send(workerActorId, new TEvContinueRequest(Ydb::StatusIds::INTERNAL_ERROR, PoolId, PoolConfig, {NYql::TIssue(TStringBuilder() << "Got duplicate session id " << sessionId << " for pool " << PoolId)}));
            return;
        }

        LOG_D("Received new request, worker id: " << workerActorId << ", session id: " << sessionId);
        if (auto cancelAfter = PoolConfig.QueryCancelAfter) {
            this->Schedule(cancelAfter, new TEvPrivate::TEvCancelRequest(sessionId));
        }

        TRequest* request = &LocalSessions.insert({sessionId, TRequest(workerActorId, sessionId)}).first->second;
        Counters.LocalDelayedRequests->Inc();

        UpdatePoolConfig(ev->Get()->PoolConfig);
        UpdateSchemeboardSubscription(ev->Get()->PathId);
        OnScheduleRequest(request);

        this->Send(MakeKqpWorkloadServiceId(this->SelfId().NodeId()), new TEvPrivate::TEvPlaceRequestIntoPoolResponse(Database, PoolId));
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
        request->Duration = ev->Get()->Duration;
        request->CpuConsumed = ev->Get()->CpuConsumed;

        LOG_D("Received cleanup request, worker id: " << workerActorId << ", session id: " << sessionId << ", duration: " << request->Duration << ", cpu consumed: " << request->CpuConsumed);
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

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
        if (ev->Get()->Key != WatchKey) {
            // Skip old paths watch notifications
            return;
        }

        const auto& result = ev->Get()->Result;
        if (!result) {
            LOG_W("Got empty notification");
            return;
        }

        if (result->GetStatus() != NKikimrScheme::StatusSuccess) {
            LOG_W("Got bad watch notification " << result->GetStatus() << ", reason: " << result->GetReason());
            return;
        }

        LOG_D("Got watch notification");

        NResourcePool::TPoolSettings poolConfig;
        ParsePoolSettings(result->GetPathDescription().GetResourcePoolDescription(), poolConfig);
        UpdatePoolConfig(poolConfig);
    }

public:
    void ReplyContinue(TRequest* request, Ydb::StatusIds::StatusCode status, const TString& message) {
        ReplyContinue(request, status, {NYql::TIssue(message)});
    }

    void ReplyContinue(TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const NYql::TIssues& issues = {}) {
        this->Send(request->WorkerActorId, new TEvContinueRequest(status, PoolId, PoolConfig, issues));

        if (status == Ydb::StatusIds::SUCCESS) {
            LocalInFlight++;
            request->Started = true;
            Counters.LocalInFly->Inc();
            Counters.ContinueOk->Inc();
            Counters.DelayedTimeMs->Collect((TInstant::Now() - request->StartTime).MilliSeconds());
            LOG_D("Reply continue success to " << request->WorkerActorId << ", session id: " << request->SessionId << ", local in flight: " << LocalInFlight);
        } else {
            if (status == Ydb::StatusIds::OVERLOADED) {
                Counters.ContinueOverloaded->Inc();
                LOG_I("Reply overloaded to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
            } else if (status == Ydb::StatusIds::CANCELLED) {
                Counters.Cancelled->Inc();
                LOG_I("Reply cancelled to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
            } else {
                Counters.ContinueError->Inc();
                LOG_W("Reply continue error " << status << " to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
            }
            RemoveRequest(request);
        }

        Counters.LocalDelayedRequests->Dec();
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
            Counters.LocalInFly->Dec();
        } else {
            Counters.LocalDelayedRequests->Dec();
        }

        if (request->State == TRequest::EState::Canceling) {
            ReplyCancel(request);
        } else {
            ReplyCleanup(request, status, issues);
        }

        RemoveRequest(request);
    }

protected:
    virtual bool ShouldResign() const = 0;
    virtual void OnScheduleRequest(TRequest* request) = 0;
    virtual void OnCleanupRequest(TRequest* request) = 0;

    virtual void RefreshState(bool refreshRequired = false) {
        Y_UNUSED(refreshRequired);
    };

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

    void RemoveRequest(TRequest* request) {
        auto event = std::make_unique<TEvPrivate::TEvFinishRequestInPool>(
            Database, PoolId, request->Duration, request->CpuConsumed, request->UsedCpuQuota
        );
        this->Send(MakeKqpWorkloadServiceId(this->SelfId().NodeId()), event.release());

        LocalSessions.erase(request->SessionId);
        if (StopHandler && LocalSessions.empty()) {
            LOG_I("All requests finished, stop handler");
            PassAway();
        }
    }

    ui64 GetLocalInFlight() const {
        return LocalInFlight;
    }

    ui64 GetLocalSessionsCount() const {
        return LocalInFlight;
    }

    TMaybe<TInstant> GetWaitDeadline(TInstant startTime) const {
        if (auto cancelAfter = PoolConfig.QueryCancelAfter) {
            return startTime + cancelAfter;
        }
        return Nothing();
    }

    TMaybe<double> GetLoadCpuThreshold() const {
        if (PoolConfig.DatabaseLoadCpuThreshold < 0.0) {
            return Nothing();
        }
        return PoolConfig.DatabaseLoadCpuThreshold;
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TPoolHandlerActorBase] ActorId: " << this->SelfId() << ", Database: " << Database << ", PoolId: " << PoolId << ", ";
    }

private:
    void ReplyCleanup(const TRequest* request, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const NYql::TIssues& issues = {}) {
        this->Send(request->WorkerActorId, new TEvCleanupResponse(status, issues));

        if (status == Ydb::StatusIds::SUCCESS) {
            Counters.CleanupOk->Inc();
            Counters.RequestsLatencyMs->Collect((TInstant::Now() - request->StartTime).MilliSeconds());
            LOG_D("Reply cleanup success to " << request->WorkerActorId << ", session id: " << request->SessionId << ", local in flight: " << LocalInFlight);
        } else {
            Counters.CleanupError->Inc();
            LOG_W("Reply cleanup error " << status << " to " << request->WorkerActorId << ", session id: " << request->SessionId << ", issues: " << issues.ToOneLineString());
        }
    }

    void ReplyCancel(const TRequest* request) {
        auto ev = std::make_unique<TEvKqp::TEvCancelQueryRequest>();
        ev->Record.MutableRequest()->SetSessionId(request->SessionId);
        this->Send(MakeKqpProxyID(this->SelfId().NodeId()), ev.release());

        Counters.Cancelled->Inc();
        Counters.RequestsLatencyMs->Collect((TInstant::Now() - request->StartTime).MilliSeconds());
        LOG_I("Cancel request for worker " << request->WorkerActorId << ", session id: " << request->SessionId << ", local in flight: " << LocalInFlight);
    }

    void UpdateSchemeboardSubscription(TPathId pathId) {
        if (WatchPathId && *WatchPathId == pathId) {
            return;
        }

        if (WatchPathId) {
            LOG_I("Pool path has changed, new path id: " << pathId.ToString());
            this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(WatchKey));
            WatchKey++;
            WatchPathId = nullptr;
        }

        LOG_D("Subscribed on schemeboard notifications for path: " << pathId.ToString());
        WatchPathId = std::make_unique<TPathId>(pathId);
        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(*WatchPathId, WatchKey));
    }

    void UpdatePoolConfig(const NResourcePool::TPoolSettings& poolConfig) {
        if (PoolConfig == poolConfig) {
            return;
        }
        LOG_D("Pool config has changed, queue size: " << poolConfig.QueueSize << ", in flight limit: " << poolConfig.ConcurrentQueryLimit);

        PoolConfig = poolConfig;
        QueueSizeLimit = GetMaxQueueSize(poolConfig);
        InFlightLimit = GetMaxInFlight(poolConfig);
        Counters.UpdateConfigCounters(poolConfig);
        RefreshState(true);

        if (ShouldResign()) {
            const TActorId& newHandler = this->RegisterWithSameMailbox(CreatePoolHandlerActor(Database, PoolId, poolConfig, Counters.CountersRoot));
            this->Send(MakeKqpWorkloadServiceId(this->SelfId().NodeId()), new TEvPrivate::TEvResignPoolHandler(Database, PoolId, newHandler));
        }
    }

    static ui64 GetMaxQueueSize(const NResourcePool::TPoolSettings& poolConfig) {
        const auto queueSize = poolConfig.QueueSize;
        return queueSize == -1 ? std::numeric_limits<ui64>::max() : static_cast<ui64>(queueSize);
    }

    static ui64 GetMaxInFlight(const NResourcePool::TPoolSettings& poolConfig) {
        const auto concurrentQueryLimit = poolConfig.ConcurrentQueryLimit;
        return concurrentQueryLimit == -1 ? std::numeric_limits<ui64>::max() : static_cast<ui64>(concurrentQueryLimit);
    }

protected:
    TCommonCounters Counters;

    // Configuration
    const TString Database;
    const TString PoolId;
    ui64 QueueSizeLimit = std::numeric_limits<ui64>::max();
    ui64 InFlightLimit = std::numeric_limits<ui64>::max();

private:
    NResourcePool::TPoolSettings PoolConfig;

    // Scheme board settings
    std::unique_ptr<TPathId> WatchPathId;
    ui64 WatchKey = 0;

    // Pool state
    ui64 LocalInFlight = 0;
    std::unordered_map<TString, TRequest> LocalSessions;
    bool StopHandler = false;  // Stop than all requests finished
};


class TUnlimitedPoolHandlerActor : public TPoolHandlerActorBase<TUnlimitedPoolHandlerActor> {
    using TBase = TPoolHandlerActorBase<TUnlimitedPoolHandlerActor>;

public:
    TUnlimitedPoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TBase::StateFuncBase, database, poolId, poolConfig, counters)
    {
        Y_ENSURE(!ShouldResign());
    }

protected:
    bool ShouldResign() const override {
        return 0 < InFlightLimit && (InFlightLimit < std::numeric_limits<ui64>::max() || GetLoadCpuThreshold());
    }

    void OnScheduleRequest(TRequest* request) override {
        ReplyContinue(request);
    }

    void OnCleanupRequest(TRequest* request) override {
        FinalReply(request);
    }
};


class TFifoPoolHandlerActor : public TPoolHandlerActorBase<TFifoPoolHandlerActor> {
    using TBase = TPoolHandlerActorBase<TFifoPoolHandlerActor>;

    struct TCounters {
        // Fifo pool counters
        NMonitoring::TDynamicCounters::TCounterPtr PendingRequestsCount;
        NMonitoring::TDynamicCounters::TCounterPtr FinishingRequestsCount;
        NMonitoring::TDynamicCounters::TCounterPtr GlobalInFly;
        NMonitoring::TDynamicCounters::TCounterPtr GlobalDelayedRequests;
        NMonitoring::THistogramPtr PoolStateUpdatesBacklogMs;

        TCounters(NMonitoring::TDynamicCounterPtr countersSubgroup) {
            Register(countersSubgroup);
        }

        void UpdateGlobalState(const TPoolStateDescription& description) {
            GlobalInFly->Set(description.RunningRequests);
            GlobalDelayedRequests->Set(description.DelayedRequests);
        }

        void OnCleanup() {
            GlobalInFly->Set(0);
            GlobalDelayedRequests->Set(0);
        }

    private:
        void Register(NMonitoring::TDynamicCounterPtr countersSubgroup) {
            PendingRequestsCount = countersSubgroup->GetCounter("PendingRequestsCount", false);
            FinishingRequestsCount = countersSubgroup->GetCounter("FinishingRequestsCount", false);
            GlobalInFly = countersSubgroup->GetCounter("GlobalInFly", false);
            GlobalDelayedRequests = countersSubgroup->GetCounter("GlobalDelayedRequests", false);
            PoolStateUpdatesBacklogMs = countersSubgroup->GetHistogram("PoolStateUpdatesBacklogMs", NMonitoring::LinearHistogram(20, 0, 3 * LEASE_DURATION.MillisecondsFloat() / 40)); 
        }
    };

    enum class EStartRequestCase {
        Pending,
        Delayed
    };

    static constexpr ui64 MAX_PENDING_REQUESTS = 1000;

public:
    TFifoPoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TFifoPoolHandlerActor::StateFunc, database, poolId, poolConfig, counters)
        , FifoCounters(Counters.CountersSubgroup)
    {
        Y_ENSURE(!ShouldResign());
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, HandleRefreshState);
            sFunc(TEvPrivate::TEvRefreshPoolState, HandleExternalRefreshState);
            hFunc(TEvPrivate::TEvCpuQuotaResponse, Handle);
            hFunc(TEvPrivate::TEvNodesInfoResponse, Handle);

            hFunc(TEvPrivate::TEvTablesCreationFinished, Handle);
            hFunc(TEvPrivate::TEvRefreshPoolStateResponse, Handle);
            hFunc(TEvPrivate::TEvDelayRequestResponse, Handle);
            hFunc(TEvPrivate::TEvStartRequestResponse, Handle);
            hFunc(TEvPrivate::TEvCleanupRequestsResponse, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void PassAway() override {
        FifoCounters.OnCleanup();

        TBase::PassAway();
    }

protected:
    bool ShouldResign() const override {
        return InFlightLimit == 0 || (InFlightLimit == std::numeric_limits<ui64>::max() && !GetLoadCpuThreshold());
    }

    void OnScheduleRequest(TRequest* request) override {
        if (PendingRequests.size() >= MAX_PENDING_REQUESTS || GetLocalSessionsCount() - GetLocalInFlight() > QueueSizeLimit + 1) {
            ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
            return;
        }

        PendingRequests.emplace_back(request->SessionId);
        FifoCounters.PendingRequestsCount->Inc();

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

    void RefreshState(bool refreshRequired = false) override {
        if (!WaitingNodesInfo && TInstant::Now() - LastNodesInfoRefreshTime > LEASE_DURATION) {
            WaitingNodesInfo = true;
            this->Send(MakeKqpWorkloadServiceId(this->SelfId().NodeId()), new TEvPrivate::TEvNodesInfoRequest());
        }

        RefreshRequired |= refreshRequired;
        if (!PreparingFinished) {
            return;
        }

        DoCleanupRequests();
        if (RunningOperation) {
            return;
        }

        RemoveFinishedRequests();
        RefreshRequired |= !PendingRequests.empty();
        RefreshRequired |= (GetLocalInFlight() || !DelayedRequests.empty()) && TInstant::Now() - LastRefreshTime > LEASE_DURATION / 4;
        RefreshRequired |= GlobalState.AmountRequests() && TInstant::Now() - LastRefreshTime > LEASE_DURATION;
        if (RefreshRequired) {
            RefreshRequired = false;
            RunningOperation = true;
            this->Register(CreateRefreshPoolStateActor(this->SelfId(), Database, PoolId, LEASE_DURATION, Counters.CountersSubgroup));
        }
    }

private:
    void HandleRefreshState() {
        RefreshScheduled = false;
        LOG_T("Try to start scheduled refresh");

        RefreshState();
        if (GetLocalSessionsCount() || GlobalState.AmountRequests()) {
            ScheduleRefresh();
        }
    }

    void HandleExternalRefreshState() {
        LOG_D("Got remote refresh request");
        RefreshState(true);
    }

    void Handle(TEvPrivate::TEvNodesInfoResponse::TPtr& ev) {
        WaitingNodesInfo = false;
        LastNodesInfoRefreshTime = TInstant::Now();
        NodeCount = ev->Get()->NodeCount;

        LOG_T("Updated node info, noode count: " << NodeCount);
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
        FifoCounters.PendingRequestsCount->Set(0);
    }

    void Handle(TEvPrivate::TEvRefreshPoolStateResponse::TPtr& ev) {
        RunningOperation = false;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("refresh pool state failed " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            RefreshRequired = true;
            ScheduleRefresh();
            return;
        }

        if (LastRefreshTime) {
            FifoCounters.PoolStateUpdatesBacklogMs->Collect((TInstant::Now() - LastRefreshTime).MilliSeconds());
        }
        LastRefreshTime = TInstant::Now();

        GlobalState = ev->Get()->PoolState;
        if (GlobalState.AmountRequests()) {
            ScheduleRefresh();
        }
        FifoCounters.UpdateGlobalState(GlobalState);
        LOG_T("succefully refreshed pool state, in flight: " << GlobalState.RunningRequests << ", delayed: " << GlobalState.DelayedRequests);

        RemoveFinishedRequests();

        size_t delayedRequestsCount = DelayedRequests.size();
        DoStartPendingRequest(GetLoadCpuThreshold());

        if (GlobalState.DelayedRequests + PendingRequests.size() > QueueSizeLimit) {
            RemoveBackRequests(PendingRequests, std::min(GlobalState.DelayedRequests + PendingRequests.size() - QueueSizeLimit, PendingRequests.size()), [this](TRequest* request) {
                ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
            });
            FifoCounters.PendingRequestsCount->Set(PendingRequests.size());
        }

        if (PendingRequests.empty() && delayedRequestsCount > QueueSizeLimit) {
            RemoveBackRequests(DelayedRequests, delayedRequestsCount - QueueSizeLimit, [this](TRequest* request) {
                AddFinishedRequest(request->SessionId);
                ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
            });
        }

        DoDelayRequest();
        DoStartDelayedRequest(GetLoadCpuThreshold());
        RefreshState();
    };

    void Handle(TEvPrivate::TEvDelayRequestResponse::TPtr& ev) {
        RunningOperation = false;
        ScheduleRefresh();

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
        FifoCounters.GlobalDelayedRequests->Inc();
        LOG_D("succefully delayed request, session id: " << ev->Get()->SessionId);

        DoStartDelayedRequest(GetLoadCpuThreshold());
        RefreshState();
    };

    void Handle(TEvPrivate::TEvCpuQuotaResponse::TPtr& ev) {
        RunningOperation = false;

        if (!ev->Get()->QuotaAccepted) {
            LOG_D("Skipped request start due to load cpu threshold");
            if (static_cast<EStartRequestCase>(ev->Cookie) == EStartRequestCase::Pending) {
                ForEachUnfinished(DelayedRequests.begin(), DelayedRequests.end(), [this](TRequest* request) {
                    AddFinishedRequest(request->SessionId);
                    ReplyContinue(request, Ydb::StatusIds::OVERLOADED, TStringBuilder() << "Too many pending requests for pool " << PoolId);
                });
            }
            RefreshState();
            return;
        }

        RemoveFinishedRequests();
        switch (static_cast<EStartRequestCase>(ev->Cookie)) {
            case EStartRequestCase::Pending:
                if (!RunningOperation && !DelayedRequests.empty()) {
                    RunningOperation = true;
                    const TString& sessionId = DelayedRequests.front();
                    this->Register(CreateStartRequestActor(this->SelfId(), Database, PoolId, sessionId, LEASE_DURATION, Counters.CountersSubgroup));
                    GetRequest(sessionId)->CleanupRequired = true;
                }
                break;

            case EStartRequestCase::Delayed:
                DoStartDelayedRequest(Nothing());
                break;
        }

        RefreshState();
    }

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

        const ui32 nodeId = ev->Get()->NodeId;
        if (SelfId().NodeId() != nodeId) {
            LOG_D("first request in queue is remote, send notification to node " << nodeId);
            auto event = std::make_unique<TEvPrivate::TEvRefreshPoolState>();
            event->Record.SetPoolId(PoolId);
            event->Record.SetDatabase(Database);
            this->Send(MakeKqpWorkloadServiceId(nodeId), std::move(event));
            RefreshState();
            return;
        }

        LOG_D("started request, session id: " << sessionId);
        bool requestFound = false;
        while (!DelayedRequests.empty() && !requestFound) {
            ForUnfinished(DelayedRequests.front(), [this, sessionId, &requestFound](TRequest* request) {
                if (request->SessionId == sessionId) {
                    request->UsedCpuQuota = !!GetLoadCpuThreshold();
                    requestFound = true;
                    GlobalState.RunningRequests++;
                    FifoCounters.GlobalInFly->Inc();
                    ReplyContinue(request);
                } else {
                    // Request was dropped due to lease expiration 
                    PendingRequests.emplace_front(request->SessionId);
                    FifoCounters.PendingRequestsCount->Inc();
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
            LOG_T("cleanuped request, session id: " << sessionId);
            if (TRequest* request = GetRequestSafe(sessionId)) {
                FinalReply(request, ev->Get()->Status, ev->Get()->Issues);
            }
        }

        RefreshState(true);
    };

private:
    void DoStartPendingRequest(TMaybe<double> loadCpuThreshold) {
        RemoveFinishedRequests();
        if (RunningOperation) {
            return;
        }

        bool canStartRequest = QueueSizeLimit == 0 && GlobalState.RunningRequests < InFlightLimit;
        canStartRequest |= !GetLoadCpuThreshold() && NodeCount && GlobalState.RunningRequests + NodeCount < InFlightLimit;
        if (!PendingRequests.empty() && canStartRequest) {
            RunningOperation = true;
            const TString& sessionId = PopPendingRequest();
            DelayedRequests.emplace_front(sessionId);
            if (loadCpuThreshold) {
                RequestCpuQuota(*loadCpuThreshold, EStartRequestCase::Pending);
            } else {
                this->Register(CreateStartRequestActor(this->SelfId(), Database, PoolId, sessionId, LEASE_DURATION, Counters.CountersSubgroup));
                GetRequest(sessionId)->CleanupRequired = true;
            }
        }
    }

    void DoStartDelayedRequest(TMaybe<double> loadCpuThreshold) {
        RemoveFinishedRequests();
        if (RunningOperation) {
            return;
        }

        if ((!DelayedRequests.empty() || GlobalState.DelayedRequests) && GlobalState.RunningRequests < InFlightLimit) {
            RunningOperation = true;
            if (loadCpuThreshold) {
                RequestCpuQuota(*loadCpuThreshold, EStartRequestCase::Delayed);
            } else {
                this->Register(CreateStartRequestActor(this->SelfId(), Database, PoolId, std::nullopt, LEASE_DURATION, Counters.CountersSubgroup));
            }
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
            this->Register(CreateDelayRequestActor(this->SelfId(), Database, PoolId, sessionId, request->StartTime, GetWaitDeadline(request->StartTime), LEASE_DURATION, Counters.CountersSubgroup));
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
            this->Register(CreateCleanupRequestsActor(this->SelfId(), Database, PoolId, FinishedRequests, Counters.CountersSubgroup));
            FinishedRequests.clear();
            FifoCounters.FinishingRequestsCount->Set(0);
        }
    }

    void ScheduleRefresh() {
        if (RefreshScheduled) {
            return;
        }
        RefreshScheduled = true;
        this->Schedule(LEASE_DURATION / 2, new TEvents::TEvWakeup());
    }

    void RequestCpuQuota(double loadCpuThreshold, EStartRequestCase requestCase) const {
        this->Send(MakeKqpWorkloadServiceId(this->SelfId().NodeId()), new TEvPrivate::TEvCpuQuotaRequest(loadCpuThreshold / 100.0), 0, static_cast<ui64>(requestCase));
    }

private:
    void RemoveFinishedRequests() {
        if (RunningOperation) {
            return;
        }

        RemoveFinishedRequests(PendingRequests);
        RemoveFinishedRequests(DelayedRequests);
        FifoCounters.PendingRequestsCount->Set(PendingRequests.size());
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

    void RemoveBackRequests(std::deque<TString>& requests, size_t countToDelete, std::function<void(TRequest*)> func) {
        auto firstRequest = requests.begin() + (requests.size() - countToDelete);
        ForEachUnfinished(firstRequest, requests.end(), func);
        requests.erase(firstRequest, requests.end());
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
        FifoCounters.PendingRequestsCount->Dec();
        return sessionId;
    }

    void AddFinishedRequest(const TString& sessionId) {
        FinishedRequests.emplace_back(sessionId);
        FifoCounters.FinishingRequestsCount->Inc();
    }

private:
    TCounters FifoCounters;

    bool PreparingFinished = false;
    bool RefreshRequired = false;
    bool RunningOperation = false;
    bool RefreshScheduled = false;

    std::deque<TString> PendingRequests;
    std::deque<TString> DelayedRequests;
    std::vector<TString> FinishedRequests;

    TInstant LastRefreshTime = TInstant::Zero();
    TPoolStateDescription GlobalState;

    bool WaitingNodesInfo = false;
    TInstant LastNodesInfoRefreshTime = TInstant::Zero();
    ui32 NodeCount = 0;
};

}  // anonymous namespace

IActor* CreatePoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters) {
    if (poolConfig.ConcurrentQueryLimit == 0 || (poolConfig.ConcurrentQueryLimit == -1 && poolConfig.DatabaseLoadCpuThreshold < 0.0)) {
        return new TUnlimitedPoolHandlerActor(database, poolId, poolConfig, counters);
    }
    return new TFifoPoolHandlerActor(database, poolId, poolConfig, counters);
}

}  // NKikimr::NKqp::NWorkload
