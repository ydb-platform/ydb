#include "private.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/folder/path.h>

namespace NKikimr::NColumnShard::NPrivate {
namespace {
class TRegularSignalBuilderActor: public NActors::TActorBootstrapped<TRegularSignalBuilderActor> {
private:
    std::shared_ptr<TValueAggregationAgent> Agent;

    void Handle(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
        Agent->ResendStatus();
        Schedule(TDuration::Seconds(5), new NActors::TEvents::TEvWakeup);
    }
public:
    TRegularSignalBuilderActor(std::shared_ptr<TValueAggregationAgent> agent)
        : Agent(agent)
    {

    }

    void Bootstrap() {
        Agent->ResendStatus();
        Schedule(TDuration::Seconds(5), new NActors::TEvents::TEvWakeup);
        Become(&TRegularSignalBuilderActor::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }
};
}


class TAggregationsControllerImpl {
private:
    TMutex Mutex;
    THashMap<TString, std::shared_ptr<TValueAggregationAgent>> Agents;
public:
    std::shared_ptr<TValueAggregationAgent> GetAggregation(const TString& signalName, const TCommonCountersOwner& signalsOwner) {
        TGuard<TMutex> g(Mutex);
        const TString agentId = TFsPath(signalsOwner.GetAggregationPathInfo() + "/" + signalName).Fix().GetPath();
        auto it = Agents.find(agentId);
        if (it == Agents.end()) {
            it = Agents.emplace(agentId, std::make_shared<TValueAggregationAgent>(signalName, signalsOwner)).first;
            if (NActors::TlsActivationContext) {
                NActors::TActivationContext::Register(new TRegularSignalBuilderActor(it->second));
            }
        }
        return it->second;
    }
};

std::shared_ptr<TValueAggregationAgent> TAggregationsController::GetAggregation(const TString& signalName, const TCommonCountersOwner& signalsOwner) {
    return Singleton<TAggregationsControllerImpl>()->GetAggregation(signalName, signalsOwner);
}

}
