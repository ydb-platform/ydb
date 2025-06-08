#include "configs_dispatcher_proxy.h"
#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr::NConsole {

class TConfigsDispatcherProxy : public TActorBootstrapped<TConfigsDispatcherProxy> {
private:
    TActorId ConfigsDispatcher;

public:
    void Bootstrap(const TActorContext& ctx) {
        ConfigsDispatcher = MakeConfigsDispatcherID(ctx.SelfID.NodeId());
        STLOG(PRI_INFO, CMS_CONFIGS, CDP01, "ConfigsDispatcher proxy started");
 
        Become(&TConfigsDispatcherProxy::StateWork);
    }

    void ForwardToConfigsDispatcher(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx)
    {
        ctx.Forward(ev, ConfigsDispatcher);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            FFunc(TEvConsole::EvGetNodeConfigurationVersionRequest, ForwardToConfigsDispatcher);
            default:
                break;
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CONFIGS_DISPATCHER_ACTOR;
    }
};

IActor* CreateConfigsDispatcherProxy() {
    return new TConfigsDispatcherProxy();
}

} // namespace NKikimr::NConsole 