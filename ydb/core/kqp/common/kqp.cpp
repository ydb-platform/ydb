#include "kqp.h"

#include <library/cpp/actors/core/actorsystem.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

bool IsSqlQuery(const NKikimrKqp::EQueryType& queryType) {
    switch (queryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_DML:
        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
        case NKikimrKqp::QUERY_TYPE_SQL_QUERY:
        case NKikimrKqp::QUERY_TYPE_FEDERATED_QUERY:
            return true;

        default:
            break;
    }

    return false;
}

TKqpShutdownController::TKqpShutdownController(NActors::TActorId kqpProxyActorId, const NKikimrConfig::TTableServiceConfig& tableServiceConfig, bool enableGraceful)
    : KqpProxyActorId_(kqpProxyActorId)
    , EnableGraceful(enableGraceful)
    , TableServiceConfig(tableServiceConfig)
{
    ShutdownState_.Reset(new TKqpShutdownState());
}

void TKqpShutdownController::Initialize(NActors::TActorSystem* actorSystem) {
    ActorSystem_ = actorSystem;
}

void TKqpShutdownController::Stop() {
    if (!EnableGraceful)
        return;

    ActorSystem_->Send(new NActors::IEventHandle(KqpProxyActorId_, {}, new TEvKqp::TEvInitiateShutdownRequest(ShutdownState_)));
    auto timeout = TDuration::MilliSeconds(TableServiceConfig.GetShutdownSettings().GetShutdownTimeoutMs());
    auto startedAt = TInstant::Now();
    auto spent = (TInstant::Now() - startedAt).SecondsFloat();
    ui32 iteration = 0;
    while (spent < timeout.SecondsFloat()) {
        if (iteration % 30 == 0) {
            Cerr << "Current KQP shutdown state: spent " << spent <<  " seconds, ";
            if (ShutdownState_->Initialized()) {
                Cerr << ShutdownState_->GetPendingSessions() << " sessions to shutdown" << Endl;
            } else {
                Cerr << "not started yet" << Endl;
            }
        }

        if (ShutdownState_->ShutdownComplete())
            break;

        Sleep(TDuration::MilliSeconds(100));
        ++iteration;
        spent = (TInstant::Now() - startedAt).SecondsFloat();
    }
}

} // namespace NKikimr::NKqp
