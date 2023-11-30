#include "db_pool.h"
#include "log.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/stream/file.h>
#include <util/string/strip.h>


namespace NDbPool {

using namespace NActors;
using namespace NYql;

class TDbPoolActor : public NActors::TActor<TDbPoolActor> {

    struct TCounters {
        const ::NMonitoring::TDynamicCounterPtr Counters;
        const NMonitoring::THistogramPtr QueueSize;
        const ::NMonitoring::TDynamicCounters::TCounterPtr TotalInFlight;
        const NMonitoring::THistogramPtr RequestsTime;
        const ::NMonitoring::TDynamicCounterPtr StatusSubgroup;
        TMap<TString, ::NMonitoring::TDynamicCounters::TCounterPtr> Status;

        TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
            , QueueSize(counters->GetSubgroup("subcomponent", "DbPool")->GetHistogram("InFlight",  NMonitoring::ExponentialHistogram(10, 2, 10)))
            , TotalInFlight(counters->GetSubgroup("subcomponent",  "DbPool")->GetCounter("TotalInflight"))
            , RequestsTime(counters->GetSubgroup("subcomponent", "DbPool")->GetHistogram("RequestTimeMs", NMonitoring::ExponentialHistogram(6, 3, 100)))
            , StatusSubgroup(counters->GetSubgroup("subcomponent", "DbPool")->GetSubgroup("component", "status"))
        {}

        ::NMonitoring::TDynamicCounters::TCounterPtr GetStatus(const NYdb::TStatus& status) {
            const TString statusStr = ToString(status.GetStatus());
            auto& counter = Status[statusStr];
            if (counter) {
                return counter;
            }
            return counter = StatusSubgroup->GetCounter(statusStr, true);
        }
    };

public:
    TDbPoolActor(
        const NYdb::NTable::TTableClient& tableClient,
        const ::NMonitoring::TDynamicCounterPtr& counters)
        : TActor(&TThis::WorkingState)
        , TableClient(tableClient)
        , Counters(counters)
    {}

    static constexpr char ActorName[] = "YQ_DB_POOL";

    STRICT_STFUNC(WorkingState,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(TEvents::TEvDbRequest, HandleRequest)
        hFunc(TEvents::TEvDbResponse, HandleResponse)
        hFunc(TEvents::TEvDbFunctionRequest, HandleRequest)
        hFunc(TEvents::TEvDbFunctionResponse, HandleResponse)
    )

    void PassAway() override {
        NYql::TIssues issues;
        issues.AddIssue("DB connection closed");
        auto cancelled = NYdb::TStatus(NYdb::EStatus::CANCELLED, std::move(issues));
        for (const auto& x : Requests) {
            if (auto pRequest = std::get_if<TRequest>(&x)) {
                Send(pRequest->Sender, new TEvents::TEvDbResponse(cancelled, {}));
            } else if (auto pRequest = std::get_if<TFunctionRequest>(&x)) {
                Send(pRequest->Sender, new TEvents::TEvDbFunctionResponse(cancelled));
            }
        }

        State.reset();
        IActor::PassAway();
    }

    void ProcessQueue() {
        Counters.QueueSize->Collect(Requests.size());
        if (Requests.empty() || RequestInProgress) {
            return;
        }
        Counters.TotalInFlight->Inc();

        RequestInProgress = true;
        RequestInProgressTimestamp = TInstant::Now();
        const auto& requestVariant = Requests.front();

        LOG_T("TDbPoolActor: ProcessQueue " << SelfId() << " Queue size = " << Requests.size());

        if (auto pRequest = std::get_if<TRequest>(&requestVariant)) {
            auto& request = *pRequest;
            auto cookie = request.Cookie;
            auto sharedResult = std::make_shared<TVector<NYdb::TResultSet>>();
            NYdb::NTable::TRetryOperationSettings settings;
            settings.Idempotent(request.Idempotent);
            TableClient.RetryOperation<NYdb::NTable::TDataQueryResult>([sharedResult, request](NYdb::NTable::TSession session) {
                return session.ExecuteDataQuery(request.Sql, NYdb::NTable::TTxControl::BeginTx(
                    NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
                    request.Params, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true))
                    .Apply([sharedResult](const auto& future) mutable {
                        *sharedResult = future.GetValue().GetResultSets();
                        return future;
                    });
            }, settings)
            .Subscribe([state = std::weak_ptr<int>(State), sharedResult, actorSystem = TActivationContext::ActorSystem(), cookie, selfId = SelfId()](const NThreading::TFuture<NYdb::TStatus>& statusFuture) {
                if (state.lock()) {
                    actorSystem->Send(new IEventHandle(selfId, selfId, new TEvents::TEvDbResponse(statusFuture.GetValue(), *sharedResult), 0, cookie));
                } else {
                    LOG_T_AS(actorSystem, "TDbPoolActor: ProcessQueue " << selfId << " State destroyed");
                }
            });
        } else if (auto pRequest = std::get_if<TFunctionRequest>(&requestVariant)) {
            auto& request = *pRequest;
            auto cookie = request.Cookie;
            TableClient.RetryOperation([request](NYdb::NTable::TSession session) {
                return request.Handler(session);
            })
            .Subscribe([state = std::weak_ptr<int>(State), actorSystem = TActivationContext::ActorSystem(), selfId = SelfId(), cookie](const NThreading::TFuture<NYdb::TStatus>& statusFuture) {
                if (state.lock()) {
                    actorSystem->Send(new IEventHandle(selfId, selfId, new TEvents::TEvDbFunctionResponse(statusFuture.GetValue()), 0, cookie));
                } else {
                    LOG_T_AS(actorSystem, "TDbPoolActor: ProcessQueue " << selfId << " State destroyed");
                }
            });
        }
    }

    void HandleRequest(TEvents::TEvDbRequest::TPtr& ev) {
        LOG_D("TDbPoolActor: TEvDbRequest " << SelfId() << " Queue size = " << Requests.size());
        auto request = ev->Get();
        Requests.emplace_back(TRequest{ev->Sender, ev->Cookie, request->Sql, std::move(request->Params), request->Idempotent});
        ProcessQueue();
    }

    void PopFromQueueAndProcess() {
        RequestInProgress = false;
        Counters.RequestsTime->Collect(TInstant::Now().MilliSeconds() - RequestInProgressTimestamp.MilliSeconds());
        Requests.pop_front();
        Counters.TotalInFlight->Dec();
        ProcessQueue();
    }

    void HandleResponse(TEvents::TEvDbResponse::TPtr& ev) {
        LOG_T("TDbPoolActor: TEvDbResponse " << SelfId() << " Queue size = " << Requests.size());
        const auto& request = Requests.front();
        Counters.GetStatus(ev.Get()->Get()->Status)->Inc();
        TActivationContext::Send(ev->Forward(std::visit([](const auto& arg) { return arg.Sender; }, request)));
        PopFromQueueAndProcess();
    }

    void HandleRequest(TEvents::TEvDbFunctionRequest::TPtr& ev) {
        LOG_T("TDbPoolActor: TEvDbFunctionRequest " << SelfId() << " Queue size = " << Requests.size());
        auto request = ev->Get();
        Requests.emplace_back(TFunctionRequest{ev->Sender, ev->Cookie, std::move(request->Handler)});
        ProcessQueue();
    }

    void HandleResponse(TEvents::TEvDbFunctionResponse::TPtr& ev) {
        LOG_T("TDbPoolActor: TEvDbFunctionResponse " << SelfId() << " Queue size = " << Requests.size());
        const auto& request = Requests.front();
        Counters.GetStatus(ev.Get()->Get()->Status)->Inc();
        TActivationContext::Send(ev->Forward(std::visit([](const auto& arg) { return arg.Sender; }, request)));
        PopFromQueueAndProcess();
    }

private:
    struct TRequest {
        TActorId Sender;
        ui64 Cookie;
        TString Sql;
        NYdb::TParams Params;
        bool Idempotent;

        TRequest() = default;
        TRequest(const TActorId sender, ui64 cookie, const TString& sql, NYdb::TParams&& params, bool idempotent)
            : Sender(sender)
            , Cookie(cookie)
            , Sql(sql)
            , Params(std::move(params))
            , Idempotent(idempotent)
        {}
    };

    struct TFunctionRequest {
        using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
        TActorId Sender;
        ui64 Cookie;
        TFunction Handler;

        TFunctionRequest() = default;
        TFunctionRequest(const TActorId sender, ui64 cookie, TFunction&& handler)
            : Sender(sender)
            , Cookie(cookie)
            , Handler(handler)
        {}
    };

    NYdb::NTable::TTableClient TableClient;
    TCounters Counters;
    TDeque<std::variant<TRequest, TFunctionRequest>> Requests;
    bool RequestInProgress = false;
    TInstant RequestInProgressTimestamp = TInstant::Now();
    std::shared_ptr<int> State = std::make_shared<int>();

};

TDbPool::TDbPool(
    ui32 sessionsCount,
    const NYdb::NTable::TTableClient& tableClient,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    const auto& ctx = NActors::TActivationContext::AsActorContext();
    auto parentId = ctx.SelfID;
    Actors.reserve(sessionsCount);
    for (ui32 i = 0; i < sessionsCount; ++i) {
        Actors.emplace_back(
            NActors::TActivationContext::Register(
                new TDbPoolActor(tableClient, counters),
                parentId, NActors::TMailboxType::HTSwap, parentId.PoolID()));
    }
}

void TDbPool::Cleanup() {
    auto parentId = NActors::TActivationContext::AsActorContext().SelfID;
    for (const auto& actorId : Actors) {
        NActors::TActivationContext::Send(new IEventHandle(actorId, parentId, new NActors::TEvents::TEvPoison()));
    }
}

TActorId TDbPool::GetNextActor() {
    TGuard<TMutex> lock(Mutex);
    Y_ENSURE(!Actors.empty());
    if (Index >= Actors.size()) {
        Index = 0;
    }

    return Actors[Index++];
}

static void PrepareConfig(NDbPool::TConfig& config) {
    if (!config.GetToken() && config.GetOAuthFile()) {
        config.SetToken(StripString(TFileInput(config.GetOAuthFile()).ReadAll()));
    }
}

TDbPoolMap::TDbPoolMap(
    const NDbPool::TConfig& config,
    NYdb::TDriver driver,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters)
    : Config(config)
    , Driver(driver)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , Counters(counters)
{
    PrepareConfig(Config);
}

TDbPoolHolder::TDbPoolHolder(
    const NDbPool::TConfig& config,
    const NYdb::TDriver& driver,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters)
    : Driver(driver)
    , Pools(new TDbPoolMap(config, Driver, credentialsProviderFactory, counters))
{ }

TDbPoolHolder::~TDbPoolHolder()
{
    Driver.Stop(true);
}

void TDbPoolHolder::Reset(const NDbPool::TConfig& config) {
    Pools->Reset(config);
}

TDbPoolMap::TPtr TDbPoolHolder::Get() {
    return Pools;
}

void TDbPoolMap::Reset(const NDbPool::TConfig& config) {
    TGuard<TMutex> lock(Mutex);
    Config = config;
    PrepareConfig(Config);
    Pools.clear();
    TableClient = nullptr;
}

TDbPool::TPtr TDbPoolHolder::GetOrCreate(ui32 dbPoolId) {
    return Pools->GetOrCreate(dbPoolId);
}

TDbPool::TPtr TDbPoolMap::GetOrCreate(ui32 dbPoolId) {
    TGuard<TMutex> lock(Mutex);
    auto it = Pools.find(dbPoolId);
    if (it != Pools.end()) {
        return it->second;
    }

    auto it_pool = Config.GetPools().find(dbPoolId);
    if (it_pool == Config.GetPools().end()) {
        return nullptr;
    }

    if (!Config.GetEndpoint()) {
        return nullptr;
    }

    if (!TableClient) {
        auto maxSessionCount = 0;
        for (const auto& pool : Config.GetPools())
        {
            maxSessionCount += pool.second;
        }
        auto clientSettings = NYdb::NTable::TClientSettings()
            .UseQueryCache(false)
            .SessionPoolSettings(NYdb::NTable::TSessionPoolSettings().MaxActiveSessions(1 + maxSessionCount))
            .Database(Config.GetDatabase())
            .DiscoveryEndpoint(Config.GetEndpoint())
            .DiscoveryMode(NYdb::EDiscoveryMode::Async);
        NKikimr::TYdbCredentialsSettings credSettings;
        credSettings.UseLocalMetadata = Config.GetUseLocalMetadataService();
        credSettings.OAuthToken = Config.GetToken();

        clientSettings.CredentialsProviderFactory(CredentialsProviderFactory(credSettings));

        clientSettings.SslCredentials(NYdb::TSslCredentials(Config.GetUseSsl()));

        TableClient = MakeHolder<NYdb::NTable::TTableClient>(Driver, clientSettings);
    }

    TDbPool::TPtr dbPool = new TDbPool(it_pool->second, *TableClient, Counters);
    Pools.emplace(dbPoolId, dbPool);
    return dbPool;
}

NYdb::TDriver& TDbPoolHolder::GetDriver() {
    return Driver;
}

NYdb::TAsyncStatus ExecDbRequest(TDbPool::TPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler) {
    NThreading::TPromise<NYdb::TStatus> promise = NThreading::NewPromise<NYdb::TStatus>();
    TActivationContext::Register(new TDbRequest(dbPool, promise, handler));
    return promise.GetFuture();
}

TDbRequest::TDbRequest(const TDbPool::TPtr& dbPool, const NThreading::TPromise<NYdb::TStatus>& promise, const TFunction& handler)
    : DbPool(dbPool)
    , Promise(promise)
    , Handler(handler)
{}

void TDbRequest::Bootstrap() {
    LOG_T("DbRequest actor request. Actor id: " << SelfId());
    Become(&TDbRequest::StateFunc);
    Send(DbPool->GetNextActor(), new TEvents::TEvDbFunctionRequest(Handler), IEventHandle::FlagTrackDelivery);
}

void TDbRequest::Handle(TEvents::TEvDbFunctionResponse::TPtr& ev) {
    LOG_T("DbRequest actor response. Actor id: " << SelfId());
    Promise.SetValue(ev->Get()->Status);
    PassAway();
}

void TDbRequest::OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&) {
    LOG_E("On delivered. Actor id: " << SelfId());
    Send(DbPool->GetNextActor(), new TEvents::TEvDbFunctionRequest(Handler), IEventHandle::FlagTrackDelivery);
}

} // namespace NDbPool
