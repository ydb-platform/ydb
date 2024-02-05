#include "jaeger_tracing_configurator.h"

#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NConsole {

class TJaegerTracingConfigurator : public TActorBootstrapped<TJaegerTracingConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::JAEGER_TRACING_CONFIGURATOR;
    }

    TJaegerTracingConfigurator(NJaegerTracing::TSamplingThrottlingConfigurator tracingConfigurator,
                               const NKikimrConfig::TTracingConfig& cfg);

    void Bootstrap(const TActorContext& ctx);

private:
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx);

    STRICT_STFUNC(StateWork,
        HFunc(TEvConsole::TEvConfigNotificationRequest, Handle)
        IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse)
    )

    TMaybe<TString> ApplyConfigs(const NKikimrConfig::TTracingConfig& cfg);

    NJaegerTracing::TSamplingThrottlingConfigurator TracingConfigurator;
};

TJaegerTracingConfigurator::TJaegerTracingConfigurator(
    NJaegerTracing::TSamplingThrottlingConfigurator tracingConfigurator,
    const NKikimrConfig::TTracingConfig& cfg)
    : TracingConfigurator(std::move(tracingConfigurator))
{
    if (auto err = ApplyConfigs(cfg)) {
        Cerr << "Failed to apply initial tracing configs: " << *err << Endl;
    }
}

void TJaegerTracingConfigurator::Bootstrap(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: Bootstrap");
    Become(&TThis::StateWork);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: subscribe to config updates");
    ui32 item = static_cast<ui32>(NKikimrConsole::TConfigItem::TracingConfigItem);
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TJaegerTracingConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
    auto& rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::CMS_CONFIGS,
               "TJaegerTracingConfigurator: got new config: "
               << rec.GetConfig().ShortDebugString());

    if (auto err = ApplyConfigs(rec.GetConfig().GetTracingConfig())) {
        LOG_NOTICE_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: Failed to apply tracing configs: " << *err);
    }

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TJaegerTracingConfigurator: Send TEvConfigNotificationResponse: "
                << resp->Record.ShortDebugString());
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

TMaybe<TString> TJaegerTracingConfigurator::ApplyConfigs(const NKikimrConfig::TTracingConfig& cfg) {
    return TracingConfigurator.HandleConfigs(cfg);
}

IActor* CreateJaegerTracingConfigurator(NJaegerTracing::TSamplingThrottlingConfigurator tracingConfigurator,
                                        const NKikimrConfig::TTracingConfig& cfg) {
    return new TJaegerTracingConfigurator(std::move(tracingConfigurator), cfg);
}

} // namespace NKikimr::NConsole
