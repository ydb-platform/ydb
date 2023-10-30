#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/ycloud/impl/grpc_service_client.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/hfunc.h>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseCache]: " << stream)

namespace NFq {

class TComputeDatabasesCacheActor : public NActors::TActorBootstrapped<TComputeDatabasesCacheActor> {
public:
    using TBase =NActors::TActorBootstrapped<TComputeDatabasesCacheActor>;
    TComputeDatabasesCacheActor(const TActorId& databaseClientActorId, const TString& databasesCacheReloadPeriod)
        : DatabaseClientActorId(databaseClientActorId)
        , DatabasesCacheReloadPeriod(GetDuration(databasesCacheReloadPeriod, TDuration::Seconds(30)))
    {}

    static constexpr char ActorName[] = "FQ_COMPUTE_DATABASES_CACHE_ACTOR";

    void Bootstrap() {
        LOG_E("Cache Bootstrap, client " << DatabaseClientActorId.ToString());
        InFlight = true;
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
        const auto& path = ev->Get()->Path;
        LOG_D("CheckDatabaseRequest, path: " << path << ", ready: " << Ready);
        if (!Ready) {
            PendingRequests.push_back(ev);
            return;
        }
        Send(ev->Sender, new TEvYdbCompute::TEvCheckDatabaseResponse(Databases.contains(path)), 0, ev->Cookie);
    }

    void Handle(TEvYdbCompute::TEvListDatabasesResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        const auto issues = response.Issues;
        InFlight = false;

        if (issues) {
            NotifyPendingRequests(issues);
            LOG_E("ListDatabasesResponse was failed with issues: " << issues.ToOneLineString());
            return;
        }

        Databases = response.Paths;
        Ready = true;
        NotifyPendingRequests();
        LOG_D("Updated list of databases, count = " << Databases.size());
    }

    void Handle(TEvYdbCompute::TEvAddDatabaseRequest::TPtr& ev) {
        const auto& path = ev->Get()->Path;
        LOG_D("AddDatabaseRequest, path: " << path << ", ready: " << Ready);
        Databases.insert(path);
        Send(ev->Sender, new TEvYdbCompute::TEvAddDatabaseResponse{}, 0, ev->Cookie);
    }

    void Wakeup() {
        if (!InFlight) {
            InFlight = true;
            Send(DatabaseClientActorId, new TEvYdbCompute::TEvListDatabasesRequest());
        }
        Schedule(DatabasesCacheReloadPeriod, new NActors::TEvents::TEvWakeup());
    }

    void NotifyPendingRequests(const NYql::TIssues& issues = {}) {
        for (const auto& request: PendingRequests) {
            if (issues) {
                Send(request->Sender, new TEvYdbCompute::TEvCheckDatabaseResponse(issues), 0, request->Cookie);
            } else {
                Send(request->Sender, new TEvYdbCompute::TEvCheckDatabaseResponse(Databases.contains(request.Get()->Get()->Path)), 0, request->Cookie);
            }
        }
        PendingRequests.clear();
    }

private:
    TVector<TEvYdbCompute::TEvCheckDatabaseRequest::TPtr> PendingRequests;
    TActorId DatabaseClientActorId;
    TSet<TString> Databases;
    bool InFlight = false;
    bool Ready = false;
    const TDuration DatabasesCacheReloadPeriod = TDuration::Seconds(30);
};

std::unique_ptr<NActors::IActor> CreateComputeDatabasesCacheActor(const TActorId& databaseClientActorId, const TString& databasesCacheReloadPeriod) {
    return std::make_unique<TComputeDatabasesCacheActor>(databaseClientActorId, databasesCacheReloadPeriod);
}

}
