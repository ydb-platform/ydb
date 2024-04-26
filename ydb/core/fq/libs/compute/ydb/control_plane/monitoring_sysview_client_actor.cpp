#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringSysViewClient]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringSysViewClient]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringSysViewClient]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringSysViewClient]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringSysViewClient]: " << stream)

namespace NFq {

using namespace NActors;

class TMonitoringSysViewServiceActor : public NActors::TActor<TMonitoringSysViewServiceActor> {
public:
    using TBase = NActors::TActor<TMonitoringSysViewServiceActor>;

    TMonitoringSysViewServiceActor(
        const ::NFq::NConfig::TYdbStorageConfig& computeConnection,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory
    )
        : TBase(&TMonitoringSysViewServiceActor::StateFunc)
        , Database(computeConnection.GetDatabase())
    {
        LOG_E("Bootstrapped for Database " << Database);
        auto tableSettings = GetClientSettings<NYdb::NTable::TClientSettings>(computeConnection, credentialsProviderFactory);
        TableClient = std::make_shared<NYdb::NTable::TTableClient>(yqSharedResources->UserSpaceYdbDriver, tableSettings);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCpuLoadRequest, Handle);
        hFunc(TEvYdbCompute::TEvCpuLoadResponse, Handle);
    )

    void Handle(TEvYdbCompute::TEvCpuLoadRequest::TPtr& ev) {
        LOG_D("CpuLoadRequest for Database " << Database);
Cerr << "CpuLoadRequest\n";
        TableClient->RetryOperation(
            [actorSystem = TActivationContext::ActorSystem(), self = SelfId(), cookie = Cookie, database = Database] (NYdb::NTable::TSession session) mutable {
Cerr << "ExecuteDataQuery\n";
                LOG_DEBUG_S(*actorSystem, NKikimrServices::FQ_RUN_ACTOR, "ExecuteDataQuery for Database " << database);
                return session.ExecuteDataQuery(
                    "SELECT AVG(CpuUsage) as Usage, AVG(CpuIdle) as Idle, SUM(CpuThreads) as Threads FROM `.sys/nodes`",
                    NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
                ).Apply([actorSystem = actorSystem, self = self, cookie = cookie, database = database](const NThreading::TFuture<NYdb::NTable::TDataQueryResult>& future) {
                        const auto& result = future.GetValue();
                        if (result.IsSuccess()) {
                            LOG_ERROR_S(*actorSystem, NKikimrServices::FQ_RUN_ACTOR, "CpuLoadRequest SUCCESS for Database " << database);
                            auto response = std::make_unique<TEvYdbCompute::TEvCpuLoadResponse>();
                            NYdb::TResultSetParser parser(result.GetResultSet(0));
                            if (parser.TryNextRow()) {
                                response->InstantLoad = parser.ColumnParser("Usage").GetDouble();
                                response->CpuNumber = parser.ColumnParser("Threads").GetUint32();
                            } else {
                                LOG_WARN_S(*actorSystem, NKikimrServices::FQ_RUN_ACTOR, "Load info is missed in nodes table for Database " << database);
                                response->Issues.AddIssue("Load info is missed in nodes table");
                            }
                            actorSystem->Send(self, response.release(), 0, cookie);
                        } else {
                            LOG_ERROR_S(*actorSystem, NKikimrServices::FQ_RUN_ACTOR, "CpuLoadRequest ERROR for Database " << database << ": " << NYdb::TStatus(result));
                        }
                        return NThreading::MakeFuture<NYdb::TStatus>(result);
                    }
                );
            }
        );
        Requests[Cookie++] = ev;
    }

    void Handle(TEvYdbCompute::TEvCpuLoadResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvCpuLoadResponse). Need to fix this bug urgently");
            return;
        }
        auto request = it->second;
        Requests.erase(it);
        Send(request->Sender, ev.Get()->Get(), 0, request->Cookie);
    }

private:
    TString Database;
    std::shared_ptr<NYdb::NTable::TTableClient> TableClient;
    TMap<uint64_t, TEvYdbCompute::TEvCpuLoadRequest::TPtr> Requests;
    int64_t Cookie = 0;
};

std::unique_ptr<NActors::IActor> CreateMonitoringSysViewServiceActor(
        const ::NFq::NConfig::TYdbStorageConfig& computeConnection,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    return std::make_unique<TMonitoringSysViewServiceActor>(
        computeConnection,
        yqSharedResources,
        credentialsProviderFactory
    );
}

}
