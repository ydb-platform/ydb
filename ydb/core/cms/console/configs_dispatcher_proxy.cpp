#include "configs_dispatcher_proxy.h"
#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT CMS_CONFIGS

namespace NKikimr::NConsole {

class TConfigsDispatcherProxy : public TActorBootstrapped<TConfigsDispatcherProxy> {
private:
    TActorId ConfigsDispatcher;

public:
    void Bootstrap(const TActorContext& ctx) {
        ConfigsDispatcher = MakeConfigsDispatcherID(ctx.SelfID.NodeId());
        YDBLOG_INFO("ConfigsDispatcher proxy started",
            {"Marker", "CDP01"});
 
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
