#include "controller.h"
#include "events.h"

namespace NKikimr::NKqp {

TKqpShutdownController::TKqpShutdownController(NActors::TActorId kqpProxyActorId, const NKikimrConfig::TTableServiceConfig& tableServiceConfig, bool enableGraceful)
    : KqpProxyActorId_(kqpProxyActorId)
    , EnableGraceful(enableGraceful)
    , TableServiceConfig(tableServiceConfig) {
    ShutdownState_.Reset(new TKqpShutdownState());
}

void TKqpShutdownController::Initialize(NActors::TActorSystem* actorSystem) {
    ActorSystem_ = actorSystem;
}

void TKqpShutdownController::Stop() {
    if (!EnableGraceful)
        return;

    ActorSystem_->Send(new NActors::IEventHandle(KqpProxyActorId_, {}, new NPrivateEvents::TEvInitiateShutdownRequest(ShutdownState_)));
    auto timeout = TDuration::MilliSeconds(TableServiceConfig.GetShutdownSettings().GetShutdownTimeoutMs());
    auto startedAt = TInstant::Now();
    auto spent = (TInstant::Now() - startedAt).SecondsFloat();
    ui32 iteration = 0;
    while (spent < timeout.SecondsFloat()) {
        if (iteration % 30 == 0) {
            Cerr << "Current KQP shutdown state: spent " << spent << " seconds, ";
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
    if (!ShutdownState_->ShutdownComplete()) {
        Cerr << "Failed to gracefully shutdown KQP after " << timeout.Seconds() << " seconds: spent " << spent << " seconds, ";
        Cerr << ShutdownState_->GetPendingSessions() << " sessions to shutdown left" << Endl;
    }
}

} // namespace NKikimr::NKqp
