#include "db_pool.h"
#include "events.h"
#include "log.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/db_pool/protos/config.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/stream/file.h>
#include <util/string/strip.h>

#include <unordered_map>
#include <unordered_set>

namespace NDbPool {

namespace {

using namespace NActors;

struct TFunctionRequest {
    using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
    TActorId Sender;
    ui64 Cookie;
    TFunction Handler;
    TInstant StartTime = TInstant::Now();

    TFunctionRequest() = default;

    TFunctionRequest(const TActorId sender, ui64 cookie, TFunction&& handler)
        : Sender(sender)
        , Cookie(cookie)
        , Handler(std::move(handler))
    {}
};

class TDbPoolActor : public TActor<TDbPoolActor> {
    using TBase = TActor<TDbPoolActor>;

    struct TCounters {
        const NMonitoring::TDynamicCounterPtr CountersSubGroup;

        const NMonitoring::THistogramPtr RequestsTime;
        const NMonitoring::TDynamicCounters::TCounterPtr SessionsCountLimit;
        const NMonitoring::TDynamicCounters::TCounterPtr ActiveTime;

        const NMonitoring::TDynamicCounterPtr StatusSubgroup;
        TMap<TString, NMonitoring::TDynamicCounters::TCounterPtr> Status;

        TCounters(NMonitoring::TDynamicCounterPtr counters)
            : CountersSubGroup(counters->GetSubgroup("subcomponent", "DbPool"))
            , RequestsTime(CountersSubGroup->GetHistogram("RequestTimeMs", NMonitoring::ExponentialHistogram(6, 3, 100)))
            , SessionsCountLimit(CountersSubGroup->GetCounter("SessionsCountLimit"))
            , ActiveTime(CountersSubGroup->GetCounter("ActiveTimeUs", true))
            , StatusSubgroup(CountersSubGroup->GetSubgroup("component", "status"))
        {
            SessionsCountLimit->Inc();
        }

        ~TCounters() {
            if (SessionsCountLimit) {
                SessionsCountLimit->Dec();
            }
        }

        NMonitoring::TDynamicCounters::TCounterPtr GetStatus(const NYdb::TStatus& status) {
            const TString statusStr = ToString(status.GetStatus());
            auto& counter = Status[statusStr];
            if (counter) {
                return counter;
            }
            return counter = StatusSubgroup->GetCounter(statusStr, true);
        }

        void ReportStats(bool newRequestInProgress) {
            const auto now = TInstant::Now();
            ActiveTime->Add(RequestInProgress * (now - LastReportTime).MicroSeconds());
            RequestInProgress = newRequestInProgress;
            LastReportTime = now;
        }

    private:
        bool RequestInProgress = false;
        TInstant LastReportTime = TInstant::Now();
    };

public:
    TDbPoolActor(NYdb::NTable::TTableClient tableClient, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TThis::WorkingState)
        , TableClient(std::move(tableClient))
        , Counters(std::move(counters))
    {}

    static constexpr char ActorName[] = "YQ_DB_POOL";

    STRICT_STFUNC(WorkingState,
        hFunc(TEvents::TEvDbFunctionRequest, Handle);
        hFunc(TEvents::TEvDbFunctionResponse, Handle);
        sFunc(NActors::TEvents::TEvWakeup, Handle);
    )

private:
    void Handle(TEvents::TEvDbFunctionRequest::TPtr& ev) {
        Y_VALIDATE(!RequestInProgress, "Can not handle requests in parallel");
        LOG_T("TEvDbFunctionRequest, has inflight: " << RequestInProgress);

        Request = TFunctionRequest{ev->Sender, ev->Cookie, std::move(ev->Get()->Handler)};
        RequestInProgress = true;
        ReportStats();

        TableClient.RetryOperation([request = Request](NYdb::NTable::TSession session) {
            return request.Handler(session);
        }).Subscribe([state = std::weak_ptr<int>(State), actorSystem = TActivationContext::ActorSystem(), selfId = SelfId(), cookie = Request.Cookie](const NThreading::TFuture<NYdb::TStatus>& statusFuture) {
            if (state.lock()) {
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvents::TEvDbFunctionResponse(statusFuture.GetValue()), 0, cookie));
            }
        });
    }

    void Handle(TEvents::TEvDbFunctionResponse::TPtr& ev) {
        Y_VALIDATE(RequestInProgress, "Unexpected worker state");
        LOG_T("TEvDbFunctionResponse, has inflight: " << RequestInProgress);

        Counters.RequestsTime->Collect((TInstant::Now() - Request.StartTime).MilliSeconds());
        Counters.GetStatus(ev->Get()->Status)->Inc();
        Send(ev->Forward(Request.Sender));

        Request = {};
        RequestInProgress = false;
        ReportStats();
    }

    void Handle() {
        WakeupScheduled = false;
        ReportStats();
    }

    void ReportStats() {
        Counters.ReportStats(RequestInProgress);

        if (!WakeupScheduled) {
            WakeupScheduled = true;
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] ActorId: " << SelfId() << ". ";
    }

    NYdb::NTable::TTableClient TableClient;
    TCounters Counters;
    TFunctionRequest Request;
    bool RequestInProgress = false;
    std::shared_ptr<int> State = std::make_shared<int>();
    bool WakeupScheduled = false;
};

class TDbPoolProxyActor : public TActor<TDbPoolProxyActor> {
    using TBase = TActor<TDbPoolProxyActor>;

    struct TCounters {
        const NMonitoring::TDynamicCounterPtr CountersSubGroup;

        const NMonitoring::THistogramPtr InFlight;
        const NMonitoring::TDynamicCounters::TCounterPtr TotalInFlight;
        const NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
        const NMonitoring::THistogramPtr QueuedTime;
        const NMonitoring::TDynamicCounters::TCounterPtr IncomingRate;
        const NMonitoring::TDynamicCounters::TCounterPtr TotalQueuedTime;

        TCounters(NMonitoring::TDynamicCounterPtr counters)
            : CountersSubGroup(counters->GetSubgroup("subcomponent", "DbPool"))
            , InFlight(CountersSubGroup->GetHistogram("InFlight",  NMonitoring::ExponentialHistogram(10, 2, 3)))
            , TotalInFlight(CountersSubGroup->GetCounter("TotalInflight"))
            , QueueSize(CountersSubGroup->GetCounter("QueueSize"))
            , QueuedTime(CountersSubGroup->GetHistogram("QueuedTimeMs", NMonitoring::ExponentialHistogram(6, 3, 10)))
            , IncomingRate(CountersSubGroup->GetCounter("IncomingRate", true))
            , TotalQueuedTime(CountersSubGroup->GetCounter("TotalQueuedTimeUs", true))
        {}

        void ReportStats(ui64 newQueuedRequests) {
            const auto now = TInstant::Now();
            TotalQueuedTime->Add(QueuedRequests * (now - LastReportTime).MicroSeconds());
            QueuedRequests = newQueuedRequests;
            LastReportTime = now;
        }

    private:
        ui64 QueuedRequests = 0;
        TInstant LastReportTime = TInstant::Now();
    };

public:
    TDbPoolProxyActor(std::unordered_set<TActorId> actors, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TThis::WorkingState)
        , Counters(std::move(counters))
        , FreeWorkers(std::move(actors))
    {}

    static constexpr char ActorName[] = "YQ_DB_POOL_PROXY";

    STRICT_STFUNC(WorkingState,
        hFunc(TEvents::TEvDbFunctionRequest, Handle);
        hFunc(TEvents::TEvDbFunctionResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
    )

    void PassAway() override {
        NYdb::NIssue::TIssues issues;
        issues.AddIssue("DB connection closed");
        const auto cancelled = NYdb::TStatus(NYdb::EStatus::CANCELLED, std::move(issues));

        for (const auto& pRequest : Requests) {
            Send(pRequest.Sender, new TEvents::TEvDbFunctionResponse(cancelled), 0, pRequest.Cookie);
        }

        for (const auto& [_, pRequest] : InflightRequests) {
            Send(pRequest.Sender, new TEvents::TEvDbFunctionResponse(cancelled), 0, pRequest.Cookie);
        }

        TBase::PassAway();
    }

private:
    void Handle(TEvents::TEvDbFunctionRequest::TPtr& ev) {
        const auto& sender = ev->Sender;
        LOG_T("TEvDbFunctionRequest from " << sender << ", Queue size = " << Requests.size());

        Counters.IncomingRate->Inc();
        Counters.QueueSize->Inc();
        Requests.emplace_back(TFunctionRequest{sender, ev->Cookie, std::move(ev->Get()->Handler)});
        ReportStats();
        ProcessQueue();
    }

    void Handle(TEvents::TEvDbFunctionResponse::TPtr& ev) {
        const auto sender = ev->Sender;
        LOG_T("TEvDbFunctionResponse from " << sender << ", Queue size = " << Requests.size());

        const auto it = InflightRequests.find(sender);
        if (it == InflightRequests.end()) {
            LOG_I("TEvDbFunctionResponse from unknown sender " << sender);
            return;
        }

        Send(ev->Forward(it->second.Sender));
        InflightRequests.erase(it);
        Y_VALIDATE(FreeWorkers.emplace(sender).second, "Unexpected worker state: " << sender);
        Counters.TotalInFlight->Dec();
        ProcessQueue();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        const auto sender = ev->Sender;
        LOG_E("Undelivered from: " << ev->Sender << ", reason: " << ev->Get()->Reason << ", Queue size = " << Requests.size());

        const auto it = InflightRequests.find(sender);
        if (it == InflightRequests.end()) {
            LOG_I("Undelivered from unknown sender " << sender);
            return;
        }

        Counters.QueueSize->Inc();
        Requests.emplace_back(it->second);
        ReportStats();

        InflightRequests.erase(it);
        Y_VALIDATE(FreeWorkers.emplace(sender).second, "Unexpected worker state: " << sender);
        Counters.TotalInFlight->Dec();
        ProcessQueue();
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        WakeupScheduled = false;
        ReportStats();
    }

    void ProcessQueue() {
        Counters.InFlight->Collect(Requests.size() + InflightRequests.size());
        if (Requests.empty() || FreeWorkers.empty()) {
            return;
        }
        Counters.TotalInFlight->Inc();

        const auto request = Requests.front();
        Requests.pop_front();
        Counters.QueueSize->Dec();
        Counters.QueuedTime->Collect((TInstant::Now() - request.StartTime).MilliSeconds());
        ReportStats();

        LOG_T("ProcessQueue, Queue size = " << Requests.size());

        const auto worker = *FreeWorkers.begin();
        FreeWorkers.erase(FreeWorkers.begin());
        Y_VALIDATE(InflightRequests.emplace(worker, request).second, "Unexpected worker state: " << worker);
        Send(worker, new TEvents::TEvDbFunctionRequest(request.Handler), 0, request.Cookie);
    }

    void ReportStats() {
        Counters.ReportStats(Requests.size());

        if (!WakeupScheduled) {
            WakeupScheduled = true;
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] ActorId: " << SelfId() << ". ";
    }

    TCounters Counters;
    TDeque<TFunctionRequest> Requests;
    std::unordered_map<TActorId, TFunctionRequest> InflightRequests;
    std::unordered_set<TActorId> FreeWorkers;
    bool WakeupScheduled = false;
};

} // anonymous namespace

class TDbPool {
    const TActorId ProxyActorId;

public:
    TDbPool(ui32 sessionsCount, NYdb::NTable::TTableClient tableClient, NMonitoring::TDynamicCounterPtr counters)
        : ProxyActorId(StartWorkers(sessionsCount, std::move(tableClient), std::move(counters)))
    {}

    TActorId GetProxyActorId() const {
        return ProxyActorId;
    }

private:
    static TActorId StartWorkers(ui32 sessionsCount, NYdb::NTable::TTableClient tableClient, NMonitoring::TDynamicCounterPtr counters) {
        Y_VALIDATE(sessionsCount > 0, "Sessions count must be greater than 0");

        const auto parentId = TActivationContext::AsActorContext().SelfID;
        std::unordered_set<TActorId> actors;
        actors.reserve(sessionsCount);
        for (ui32 i = 0; i < sessionsCount; ++i) {
            Y_VALIDATE(actors.emplace(TActivationContext::Register(new TDbPoolActor(tableClient, counters), parentId, TMailboxType::HTSwap, parentId.PoolID())).second, "Unexpected actor id");
        }

        return TActivationContext::Register(new TDbPoolProxyActor(std::move(actors), std::move(counters)), parentId, TMailboxType::HTSwap, parentId.PoolID());
    }
};

namespace {

class TDbRequest : public TActorBootstrapped<TDbRequest> {
    using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;

    const TDbPoolPtr DbPool;
    const TFunction Handler;
    NThreading::TPromise<NYdb::TStatus> Promise;

public:
    static constexpr char ActorName[] = "YQ_DB_REQUEST";

    TDbRequest(TDbPoolPtr dbPool, NThreading::TPromise<NYdb::TStatus> promise, TFunction handler)
        : DbPool(std::move(dbPool))
        , Handler(std::move(handler))
        , Promise(std::move(promise))
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvDbFunctionResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

    void Bootstrap() {
        const auto& proxyId = DbPool->GetProxyActorId();
        LOG_T("Bootstrap, send request to: " << proxyId);
        Become(&TDbRequest::StateFunc);
        Send(proxyId, new TEvents::TEvDbFunctionRequest(Handler), IEventHandle::FlagTrackDelivery);
    }

private:
    void Handle(TEvents::TEvDbFunctionResponse::TPtr& ev) {
        const auto status = ev->Get()->Status;
        LOG_T("DbRequest actor response from: " << ev->Sender << ", status: " << status);
        Promise.SetValue(status);
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        const auto& proxyId = DbPool->GetProxyActorId();
        LOG_E("Undelivered from: " << ev->Sender << ", reason: " << ev->Get()->Reason << ", resend request to: " << proxyId);
        Send(proxyId, new TEvents::TEvDbFunctionRequest(Handler), IEventHandle::FlagTrackDelivery);
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] ActorId: " << SelfId() << ". ";
    }
};

} // anonymous namespace

class TDbPoolMap final : public TThrRefBase {
    const NDbPool::TConfig Config;
    const NYdb::TDriver Driver;
    const NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    const NMonitoring::TDynamicCounterPtr Counters;
    THashMap<ui32, TDbPoolPtr> Pools;
    THolder<NYdb::NTable::TTableClient> TableClient;
    TMutex Mutex;

public:
    using TPtr = TIntrusivePtr<TDbPoolMap>;

    TDbPoolMap(const NDbPool::TConfig& config, NYdb::TDriver driver, NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory, NMonitoring::TDynamicCounterPtr counters)
        : Config(PrepareConfig(config))
        , Driver(driver)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Counters(counters)
    {}

    TDbPoolPtr GetOrCreate(ui32 poolId) {
        TGuard<TMutex> lock(Mutex);

        if (const auto it = Pools.find(poolId); it != Pools.end()) {
            return it->second;
        }

        const auto it = Config.GetPools().find(poolId);
        if (it == Config.GetPools().end() || !Config.GetEndpoint()) {
            return nullptr;
        }

        if (!TableClient) {
            ui32 maxSessionCount = 0;
            for (const auto& pool : Config.GetPools()) {
                maxSessionCount += pool.second;
            }

            NKikimr::TYdbCredentialsSettings credSettings;
            credSettings.UseLocalMetadata = Config.GetUseLocalMetadataService();
            credSettings.OAuthToken = Config.GetToken();

            TableClient = MakeHolder<NYdb::NTable::TTableClient>(Driver, NYdb::NTable::TClientSettings()
                .UseQueryCache(false)
                .SessionPoolSettings(NYdb::NTable::TSessionPoolSettings().MaxActiveSessions(1 + maxSessionCount))
                .Database(Config.GetDatabase())
                .DiscoveryEndpoint(Config.GetEndpoint())
                .DiscoveryMode(NYdb::EDiscoveryMode::Async)
                .SslCredentials(NYdb::TSslCredentials(Config.GetUseSsl()))
                .CredentialsProviderFactory(CredentialsProviderFactory(credSettings))
            );
        }

        auto dbPool = std::make_shared<TDbPool>(it->second, *TableClient, Counters);
        Pools.emplace(poolId, dbPool);
        return dbPool;
    }

private:
    static NDbPool::TConfig PrepareConfig(const NDbPool::TConfig& config) {
        NDbPool::TConfig result = config;
        if (!result.GetToken() && result.GetOAuthFile()) {
            result.SetToken(StripString(TFileInput(result.GetOAuthFile()).ReadAll()));
        }
        return result;
    }
};

TDbPoolHolder::TDbPoolHolder(const NDbPool::TConfig& config, NYdb::TDriver driver, NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory, NMonitoring::TDynamicCounterPtr counters)
    : Driver(std::move(driver))
    , Pools(MakeIntrusive<TDbPoolMap>(config, Driver, std::move(credentialsProviderFactory), std::move(counters)))
{}

TDbPoolHolder::~TDbPoolHolder() {
    try {
        Driver.Stop(true);
    } catch (...) {
        // Stop fail
    }
}

TDbPoolPtr TDbPoolHolder::GetOrCreate(ui32 dbPoolId) {
    return Pools->GetOrCreate(dbPoolId);
}

NYdb::TAsyncStatus ExecDbRequest(TDbPoolPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler) {
    Y_VALIDATE(dbPool, "Missing db pool");
    const auto parentId = TActivationContext::AsActorContext().SelfID;
    NThreading::TPromise<NYdb::TStatus> promise = NThreading::NewPromise<NYdb::TStatus>();
    TActivationContext::Register(new TDbRequest(std::move(dbPool), promise, std::move(handler)), parentId, TMailboxType::HTSwap, parentId.PoolID());
    return promise.GetFuture();
}

} // namespace NDbPool
