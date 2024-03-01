#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)

namespace NFq {

class TComputeDatabasesCacheActor : public NActors::TActorBootstrapped<TComputeDatabasesCacheActor> {
    struct TCounters {
        ::NMonitoring::TDynamicCounterPtr Counters;
        struct TCommonMetrics {
            ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
            ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
            ::NMonitoring::TDynamicCounters::TCounterPtr Error;
            ::NMonitoring::THistogramPtr LatencyMs;
        };

        TCommonMetrics CheckDatabaseRequest;
        TCommonMetrics AddDatabaseRequest;
        TCommonMetrics CacheReload;

        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            Register();
        }

    private:
        void Register() {
            ::NMonitoring::TDynamicCounterPtr component = Counters->GetSubgroup("component", "ComputeDatabasesCache");
            RegisterCommonMetrics(CheckDatabaseRequest, component->GetSubgroup("subcomponent", "CheckDatabaseRequest"));
            RegisterCommonMetrics(AddDatabaseRequest, component->GetSubgroup("subcomponent", "AddDatabaseRequest"));
            RegisterCommonMetrics(CacheReload, component->GetSubgroup("subcomponent", "CacheReload"));
        }

        void RegisterCommonMetrics(TCommonMetrics& metrics, ::NMonitoring::TDynamicCounterPtr subComponent) {
            metrics.Ok = subComponent->GetCounter("Ok", true);
            metrics.Error = subComponent->GetCounter("Error", true);
            metrics.InFly = subComponent->GetCounter("InFly", false);
            metrics.LatencyMs = subComponent->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
        }

        static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
            return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
        }
    };

    struct TPendingItem {
        TInstant StartTime;
        TEvYdbCompute::TEvCheckDatabaseRequest::TPtr Request;
    };

public:
    using TBase =NActors::TActorBootstrapped<TComputeDatabasesCacheActor>;
    TComputeDatabasesCacheActor(const TActorId& databaseClientActorId, const TString& databasesCacheReloadPeriod, const ::NMonitoring::TDynamicCounterPtr& counters)
        : StartCacheReload(TInstant::Now())
        , DatabaseClientActorId(databaseClientActorId)
        , Counters(counters)
        , DatabasesCacheReloadPeriod(GetDuration(databasesCacheReloadPeriod, TDuration::Seconds(30)))
    {}

    static constexpr char ActorName[] = "FQ_COMPUTE_DATABASES_CACHE_ACTOR";

    void Bootstrap() {
        LOG_E("Cache Bootstrap, client " << DatabaseClientActorId.ToString());
        InFlight = true;
        Counters.CacheReload.InFly->Inc();
        Send(DatabaseClientActorId, new TEvYdbCompute::TEvListDatabasesRequest());
        Become(&TComputeDatabasesCacheActor::StateFunc, DatabasesCacheReloadPeriod, new NActors::TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvListDatabasesResponse, Handle);
        hFunc(TEvYdbCompute::TEvCheckDatabaseRequest, Handle);
        hFunc(TEvYdbCompute::TEvAddDatabaseRequest, Handle);
        cFunc(NActors::TEvents::TEvWakeup::EventType, Wakeup);
    )

    void Handle(TEvYdbCompute::TEvCheckDatabaseRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        Counters.CheckDatabaseRequest.InFly->Inc();
        const auto& path = ev->Get()->Path;
        LOG_D("CheckDatabaseRequest, path: " << path << ", ready: " << Ready);
        if (!Ready) {
            PendingRequests.push_back({startTime, ev});
            return;
        }

        Send(ev->Sender, new TEvYdbCompute::TEvCheckDatabaseResponse(Databases.contains(path)), 0, ev->Cookie);
        Counters.CheckDatabaseRequest.InFly->Dec();
        Counters.CheckDatabaseRequest.Ok->Inc();
        Counters.CheckDatabaseRequest.LatencyMs->Collect(DeltaMs(startTime));
    }

    void Handle(TEvYdbCompute::TEvListDatabasesResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        const auto issues = response.Issues;
        InFlight = false;

        if (issues) {
            NotifyPendingRequests(issues);
            LOG_E("ListDatabasesResponse was failed with issues: " << issues.ToOneLineString());
            Counters.CacheReload.Error->Inc();
            Counters.CacheReload.InFly->Dec();
            Counters.CacheReload.LatencyMs->Collect(DeltaMs(StartCacheReload));
            return;
        }

        Databases = response.Paths;
        Ready = true;
        NotifyPendingRequests();
        LOG_D("Updated list of databases, count = " << Databases.size());
        Counters.CacheReload.Ok->Inc();
        Counters.CacheReload.InFly->Dec();
        Counters.CacheReload.LatencyMs->Collect(DeltaMs(StartCacheReload));
    }

    void Handle(TEvYdbCompute::TEvAddDatabaseRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        Counters.AddDatabaseRequest.InFly->Inc();
        const auto& path = ev->Get()->Path;
        LOG_D("AddDatabaseRequest, path: " << path << ", ready: " << Ready);
        Databases.insert(path);
        Send(ev->Sender, new TEvYdbCompute::TEvAddDatabaseResponse{}, 0, ev->Cookie);
        Counters.AddDatabaseRequest.InFly->Dec();
        Counters.AddDatabaseRequest.Ok->Inc();
        Counters.AddDatabaseRequest.LatencyMs->Collect(DeltaMs(startTime));
    }

    void Wakeup() {
        if (!InFlight) {
            Counters.CacheReload.InFly->Inc();
            InFlight = true;
            StartCacheReload = TInstant::Now();
            Send(DatabaseClientActorId, new TEvYdbCompute::TEvListDatabasesRequest());
        }
        Schedule(DatabasesCacheReloadPeriod, new NActors::TEvents::TEvWakeup());
    }

    void NotifyPendingRequests(const NYql::TIssues& issues = {}) {
        for (const auto& item: PendingRequests) {
            if (issues) {
                Send(item.Request->Sender, new TEvYdbCompute::TEvCheckDatabaseResponse(issues), 0, item.Request->Cookie);
                Counters.CheckDatabaseRequest.Error->Inc();
            } else {
                Send(item.Request->Sender, new TEvYdbCompute::TEvCheckDatabaseResponse(Databases.contains(item.Request.Get()->Get()->Path)), 0, item.Request->Cookie);
                Counters.CheckDatabaseRequest.Ok->Inc();
            }
            Counters.CheckDatabaseRequest.InFly->Dec();
            Counters.CheckDatabaseRequest.LatencyMs->Collect(DeltaMs(item.StartTime));
        }
        PendingRequests.clear();
    }

private:
    static ui64 DeltaMs(const TInstant& startTime) {
        return (TInstant::Now() - startTime).MilliSeconds();
    }

private:
    TVector<TPendingItem> PendingRequests;
    TInstant StartCacheReload;
    TActorId DatabaseClientActorId;
    TSet<TString> Databases;
    TCounters Counters;
    bool InFlight = false;
    bool Ready = false;
    const TDuration DatabasesCacheReloadPeriod = TDuration::Seconds(30);
};

std::unique_ptr<NActors::IActor> CreateComputeDatabasesCacheActor(const TActorId& databaseClientActorId, const TString& databasesCacheReloadPeriod, const ::NMonitoring::TDynamicCounterPtr& counters) {
    return std::make_unique<TComputeDatabasesCacheActor>(databaseClientActorId, databasesCacheReloadPeriod, counters);
}

}
