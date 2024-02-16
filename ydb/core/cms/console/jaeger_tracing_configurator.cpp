#include "jaeger_tracing_configurator.h"

#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/settings.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NConsole {

class TJaegerTracingConfigurator : public TActorBootstrapped<TJaegerTracingConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::JAEGER_TRACING_CONFIGURATOR;
    }

    TJaegerTracingConfigurator(NJaegerTracing::TSamplingThrottlingConfigurator tracingConfigurator,
                               NKikimrConfig::TTracingConfig cfg);

    void Bootstrap(const TActorContext& ctx);

private:
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx);

    STRICT_STFUNC(StateWork,
        HFunc(TEvConsole::TEvConfigNotificationRequest, Handle)
        IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse)
    )

    void ApplyConfigs(const NKikimrConfig::TTracingConfig& cfg);
    static TMaybe<NJaegerTracing::ERequestType> GetRequestType(const NKikimrConfig::TTracingConfig::TSelectors& selectors);
    static NJaegerTracing::TSettings<double, NJaegerTracing::TThrottlingSettings> GetSettings(const NKikimrConfig::TTracingConfig& cfg);

    NJaegerTracing::TSamplingThrottlingConfigurator TracingConfigurator;
    NKikimrConfig::TTracingConfig initialConfig;
};

TJaegerTracingConfigurator::TJaegerTracingConfigurator(
    NJaegerTracing::TSamplingThrottlingConfigurator tracingConfigurator,
    NKikimrConfig::TTracingConfig cfg)
    : TracingConfigurator(std::move(tracingConfigurator))
    , initialConfig(std::move(cfg))
{}

void TJaegerTracingConfigurator::Bootstrap(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: Bootstrap");
    Become(&TThis::StateWork);

    ApplyConfigs(initialConfig);

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

    ApplyConfigs(rec.GetConfig().GetTracingConfig());

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TJaegerTracingConfigurator: Send TEvConfigNotificationResponse");
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TJaegerTracingConfigurator::ApplyConfigs(const NKikimrConfig::TTracingConfig& cfg) {
    auto settings = GetSettings(cfg);
    return TracingConfigurator.UpdateSettings(std::move(settings));
}

TMaybe<NJaegerTracing::ERequestType> TJaegerTracingConfigurator::GetRequestType(const NKikimrConfig::TTracingConfig::TSelectors& selectors) {
    if (!selectors.HasRequestType()) {
        return NJaegerTracing::ERequestType::UNSPECIFIED;
    }
    if (auto it = NJaegerTracing::NameToRequestType.FindPtr(selectors.GetRequestType())) {
        return *it;
    }
    return {};
}

NJaegerTracing::TSettings<double, NJaegerTracing::TThrottlingSettings> TJaegerTracingConfigurator::GetSettings(const NKikimrConfig::TTracingConfig& cfg) {
    NJaegerTracing::TSettings<double, NJaegerTracing::TThrottlingSettings> settings;

    for (const auto& samplingRule : cfg.GetSampling()) {
        NJaegerTracing::ERequestType requestType;
        if (auto parsedRequestType = GetRequestType(samplingRule.GetScope())) {
            requestType = *parsedRequestType;
        } else {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "failed to parse request type in rule "
                       << samplingRule.DebugString() << ". Skipping the rule");
            continue;
        }
        if (!samplingRule.HasLevel() || !samplingRule.HasFraction() || samplingRule.HasMaxRatePerMinute()) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "missing required fields in rule " << samplingRule.DebugString()
                       << " (required fields are: level, fraction, max_rate_per_minute). Skipping the rule");
            continue;
        }
        ui64 level = samplingRule.GetLevel();
        double fraction = samplingRule.GetFraction();
        if (level > 15) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "sampling level exceeds maximum allowed value (" << level
                       << " provided, maximum is 15). Lowering the level");
            level = 15;
        }
        if (fraction < 0 || fraction > 1) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "provided fraction " << fraction
                       << " violated range [0; 1]. Clamping it to the range");
            fraction = std::min(1.0, std::max(0.0, fraction));
        }

        NJaegerTracing::TSamplingRule<double, NJaegerTracing::TThrottlingSettings> rule {
            .Level = static_cast<ui8>(level),
            .Sampler = fraction,
            .Throttler = NJaegerTracing::TThrottlingSettings {
                .MaxRatePerMinute = samplingRule.GetMaxRatePerMinute(),
                .MaxBurst = samplingRule.GetMaxBurst(),
            },
        };
        settings.SamplingRules[static_cast<size_t>(requestType)].push_back(rule);
    }

    for (const auto& throttlingRule : cfg.GetExternalThrottling()) {
        NJaegerTracing::ERequestType requestType;
        if (auto parsedRequestType = GetRequestType(throttlingRule.GetScope())) {
            requestType = *parsedRequestType;
        } else {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "failed to parse request type in rule "
                       << throttlingRule.DebugString() << ". Skipping the rule");
            continue;
        }

        if (!throttlingRule.HasMaxRatePerMinute()) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "missing required field max_rate_per_minute in rule "
                       << throttlingRule.DebugString() << ". Skipping the rule");
            continue;
        }

        ui64 maxRatePerMinute = throttlingRule.GetMaxRatePerMinute();
        ui64 maxBurst = throttlingRule.GetMaxBurst();
        NJaegerTracing::TExternalThrottlingRule<NJaegerTracing::TThrottlingSettings> rule {
            .Throttler = NJaegerTracing::TThrottlingSettings {
                .MaxRatePerMinute = maxRatePerMinute,
                .MaxBurst = maxBurst,
            },
        };
        auto& currentRule = settings.ExternalThrottlingRules[static_cast<size_t>(requestType)];
        if (currentRule) {
            ALOG_WARN(NKikimrServices::CMS_CONFIGS, "duplicate external throttling rule for scope "
                      << throttlingRule.GetScope() << ". Adding the limits");
            currentRule->Throttler.MaxBurst += rule.Throttler.MaxBurst;
            currentRule->Throttler.MaxRatePerMinute += rule.Throttler.MaxRatePerMinute;
        } else {
            currentRule = rule;
        }
    }

    return settings;
}

IActor* CreateJaegerTracingConfigurator(NJaegerTracing::TSamplingThrottlingConfigurator tracingConfigurator,
                                        NKikimrConfig::TTracingConfig cfg) {
    return new TJaegerTracingConfigurator(std::move(tracingConfigurator), std::move(cfg));
}

} // namespace NKikimr::NConsole
