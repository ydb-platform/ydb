#include "jaeger_tracing_configurator.h"

#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/settings.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NConsole {

using namespace NJaegerTracing;

class TJaegerTracingConfigurator : public TActorBootstrapped<TJaegerTracingConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::JAEGER_TRACING_CONFIGURATOR;
    }

    TJaegerTracingConfigurator(TSamplingThrottlingConfigurator tracingConfigurator,
                               NKikimrConfig::TTracingConfig cfg);

    void Bootstrap(const TActorContext& ctx);

private:
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx);

    STRICT_STFUNC(StateWork,
        HFunc(TEvConsole::TEvConfigNotificationRequest, Handle)
        IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse)
    )

    void ApplyConfigs(const NKikimrConfig::TTracingConfig& cfg);
    static TVector<ERequestType> GetRequestTypes(const NKikimrConfig::TTracingConfig::TSelectors& selectors);
    static TMaybe<TString> GetDatabase(const NKikimrConfig::TTracingConfig::TSelectors& selectors);
    static TSettings<double, TWithTag<TThrottlingSettings>> GetSettings(const NKikimrConfig::TTracingConfig& cfg);

    TSamplingThrottlingConfigurator TracingConfigurator;
    NKikimrConfig::TTracingConfig initialConfig;
};

TJaegerTracingConfigurator::TJaegerTracingConfigurator(
    TSamplingThrottlingConfigurator tracingConfigurator,
    NKikimrConfig::TTracingConfig cfg)
    : TracingConfigurator(std::move(tracingConfigurator))
    , initialConfig(std::move(cfg))
{}

void TJaegerTracingConfigurator::Bootstrap(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: Bootstrap");
    Become(&TThis::StateWork);

    ApplyConfigs(initialConfig);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: subscribing to config updates");
    ui32 item = static_cast<ui32>(NKikimrConsole::TConfigItem::TracingConfigItem);
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TJaegerTracingConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
    auto& rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::CMS_CONFIGS, "TJaegerTracingConfigurator: got new config: " << rec.GetConfig().ShortDebugString());

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

TVector<ERequestType> TJaegerTracingConfigurator::GetRequestTypes(const NKikimrConfig::TTracingConfig::TSelectors& selectors) {
    TVector<ERequestType> requestTypes;
    bool hasErrors = false;
    for (const auto& requestType: selectors.GetRequestTypes()) {
        if (auto it = NameToRequestType.FindPtr(requestType)) {
            requestTypes.push_back(*it);
        } else {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "Failed to parse request type \"" << requestType << "\"");
            hasErrors = true;
        }
    }

    if (hasErrors) {
        return  {};
    }
    if (requestTypes.empty()) {
        requestTypes.push_back(ERequestType::UNSPECIFIED);
    }
    return requestTypes;
}

TMaybe<TString> TJaegerTracingConfigurator::GetDatabase(const NKikimrConfig::TTracingConfig::TSelectors& selectors) {
    if (selectors.HasDatabase()) {
        return selectors.GetDatabase();
    }
    return NothingObject;
}

TSettings<double, TWithTag<TThrottlingSettings>> TJaegerTracingConfigurator::GetSettings(const NKikimrConfig::TTracingConfig& cfg) {
    TSettings<double, TWithTag<TThrottlingSettings>> settings;

    size_t tag = 0;

    for (const auto& samplingRule : cfg.GetSampling()) {
        const auto& scope = samplingRule.GetScope();

        auto requestTypes = GetRequestTypes(scope);
        if (requestTypes.empty()) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "failed to parse request type in the rule "
                       << samplingRule.ShortDebugString() << ". Skipping the rule");
            continue;
        }

        if (!samplingRule.HasLevel() || !samplingRule.HasFraction() || !samplingRule.HasMaxTracesPerMinute()) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "missing required fields in rule " << samplingRule.ShortDebugString()
                       << " (required fields are: level, fraction, max_traces_per_minute). Skipping the rule");
            continue;
        }
        if (samplingRule.GetMaxTracesPerMinute() == 0) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "max_traces_per_minute should never be zero. Found in rule " << samplingRule.GetMaxTracesPerMinute()
                       << ". Skipping the rule");
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

        TSamplingRule<double, TWithTag<TThrottlingSettings>> rule {
            .Level = static_cast<ui8>(level),
            .Sampler = fraction,
            .Throttler = TWithTag<TThrottlingSettings> {
                .Value = TThrottlingSettings {
                    .MaxTracesPerMinute = samplingRule.GetMaxTracesPerMinute(),
                    .MaxTracesBurst = samplingRule.GetMaxTracesBurst(),
                },
                .Tag = tag++,
            },
        };

        for (auto requestType: requestTypes) {
            auto& requestTypeRules = settings.SamplingRules[static_cast<size_t>(requestType)];
            auto database = GetDatabase(scope);
            if (database) {
                requestTypeRules.DatabaseRules[*database].push_back(rule);
            } else {
                requestTypeRules.Global.push_back(rule);
            }
        }
    }

    for (const auto& throttlingRule : cfg.GetExternalThrottling()) {
        const auto& scope = throttlingRule.GetScope();

        auto requestTypes = GetRequestTypes(throttlingRule.GetScope());
        if (requestTypes.empty()) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "failed to parse request type in rule "
                       << throttlingRule.ShortDebugString() << ". Skipping the rule");
            continue;
        }

        if (!throttlingRule.HasMaxTracesPerMinute()) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "missing required field max_traces_per_minute in rule "
                       << throttlingRule.ShortDebugString() << ". Skipping the rule");
            continue;
        }
        if (throttlingRule.GetMaxTracesPerMinute() == 0) {
            ALOG_ERROR(NKikimrServices::CMS_CONFIGS, "max_traces_per_minute should never be zero. Found in rule " << throttlingRule.GetMaxTracesPerMinute()
                       << ". Skipping the rule");
            continue;
        }

        ui64 maxRatePerMinute = throttlingRule.GetMaxTracesPerMinute();
        ui64 maxBurst = throttlingRule.GetMaxTracesBurst();
        TExternalThrottlingRule<TWithTag<TThrottlingSettings>> rule {
            .Throttler = TWithTag<TThrottlingSettings> {
                .Value = TThrottlingSettings {
                    .MaxTracesPerMinute = maxRatePerMinute,
                    .MaxTracesBurst = maxBurst,
                },
                .Tag = tag++,
            }
        };

        for (auto requestType : requestTypes) {
            auto& requestTypeRules = settings.ExternalThrottlingRules[static_cast<size_t>(requestType)];
            auto database = GetDatabase(scope);
            if (database) {
                requestTypeRules.DatabaseRules[*database].push_back(rule);
            } else {
                requestTypeRules.Global.push_back(rule);
            }
        }
    }

    return settings;
}

IActor* CreateJaegerTracingConfigurator(TSamplingThrottlingConfigurator tracingConfigurator,
                                        NKikimrConfig::TTracingConfig cfg) {
    return new TJaegerTracingConfigurator(std::move(tracingConfigurator), std::move(cfg));
}

} // namespace NKikimr::NConsole
