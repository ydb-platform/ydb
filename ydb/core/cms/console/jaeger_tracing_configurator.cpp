#include "jaeger_tracing_configurator.h"

#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/settings.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

namespace NKikimr::NConsole {

using namespace NJaegerTracing;

class TJaegerTracingConfigurator : public TActorBootstrapped<TJaegerTracingConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::JAEGER_TRACING_CONFIGURATOR;
    }

    TJaegerTracingConfigurator(TIntrusivePtr<TSamplingThrottlingConfigurator> tracingConfigurator,
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

    TIntrusivePtr<TSamplingThrottlingConfigurator> TracingConfigurator;
    NKikimrConfig::TTracingConfig initialConfig;
};

TJaegerTracingConfigurator::TJaegerTracingConfigurator(
    TIntrusivePtr<TSamplingThrottlingConfigurator> tracingConfigurator,
    NKikimrConfig::TTracingConfig cfg)
    : TracingConfigurator(std::move(tracingConfigurator))
    , initialConfig(std::move(cfg))
{}

void TJaegerTracingConfigurator::Bootstrap(const TActorContext& ctx) {
    YDB_LOG_CTX_DEBUG(ctx, "TJaegerTracingConfigurator: Bootstrap");
    Become(&TThis::StateWork);

    ApplyConfigs(initialConfig);

    YDB_LOG_CTX_DEBUG(ctx, "TJaegerTracingConfigurator: subscribing to config updates");
    ui32 item = static_cast<ui32>(NKikimrConsole::TConfigItem::TracingConfigItem);
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TJaegerTracingConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
    auto& rec = ev->Get()->Record;

    YDB_LOG_CTX_INFO(ctx, "TJaegerTracingConfigurator: got new",
        {"config", rec.GetConfig().ShortDebugString()});

    ApplyConfigs(rec.GetConfig().GetTracingConfig());

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
    YDB_LOG_CTX_TRACE(ctx, "TJaegerTracingConfigurator: Send TEvConfigNotificationResponse");
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TJaegerTracingConfigurator::ApplyConfigs(const NKikimrConfig::TTracingConfig& cfg) {
    auto settings = GetSettings(cfg);
    return TracingConfigurator->UpdateSettings(std::move(settings));
}

TVector<ERequestType> TJaegerTracingConfigurator::GetRequestTypes(const NKikimrConfig::TTracingConfig::TSelectors& selectors) {
    TVector<ERequestType> requestTypes;
    bool hasErrors = false;
    for (const auto& requestType: selectors.GetRequestTypes()) {
        if (auto it = NameToRequestType.FindPtr(requestType)) {
            requestTypes.push_back(*it);
        } else {
            YDB_LOG_ERROR("Failed to parse request type",
                {"requestType", requestType});
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
            YDB_LOG_ERROR("failed to parse request type in the rule. Skipping the rule",
                {"ShortDebugString", samplingRule.ShortDebugString()});
            continue;
        }

        if (!samplingRule.HasLevel() || !samplingRule.HasFraction() || !samplingRule.HasMaxTracesPerMinute()) {
            YDB_LOG_ERROR("missing required fields in rule (required fields are: level, fraction, max_traces_per_minute). Skipping the rule",
                {"ShortDebugString", samplingRule.ShortDebugString()});
            continue;
        }
        if (samplingRule.GetMaxTracesPerMinute() == 0) {
            YDB_LOG_ERROR("max_traces_per_minute should never be zero. Found in rule. Skipping the rule",
                {"GetMaxTracesPerMinute", samplingRule.GetMaxTracesPerMinute()});
            continue;
        }

        ui64 level = samplingRule.GetLevel();
        double fraction = samplingRule.GetFraction();
        if (level > TComponentTracingLevels::MostVerbose) {
            YDB_LOG_ERROR("sampling level exceeds maximum allowed value ( provided, maximum is ). Lowering the level",
                {"level", level},
                {"#_static_cast<ui32>(TComponentTracingLevels::MostVerbose)", static_cast<ui32>(TComponentTracingLevels::MostVerbose)});
            level = TComponentTracingLevels::MostVerbose;
        }
        if (fraction < 0 || fraction > 1) {
            YDB_LOG_ERROR("provided fraction violated range [0; 1]. Clamping it to the range",
                {"fraction", fraction});
            fraction = std::clamp(fraction, 0.0, 1.0);
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
            YDB_LOG_ERROR("failed to parse request type in rule. Skipping the rule",
                {"ShortDebugString", throttlingRule.ShortDebugString()});
            continue;
        }

        ui64 level = throttlingRule.HasLevel() ? throttlingRule.GetLevel() : TComponentTracingLevels::ProductionVerbose;
        if (level > TComponentTracingLevels::MostVerbose) {
            YDB_LOG_ERROR("sampling level exceeds maximum allowed value ( provided, maximum is ). Lowering the level",
                {"level", level},
                {"#_static_cast<ui32>(TComponentTracingLevels::MostVerbose)", static_cast<ui32>(TComponentTracingLevels::MostVerbose)});
            level = TComponentTracingLevels::MostVerbose;
        }

        if (!throttlingRule.HasMaxTracesPerMinute()) {
            YDB_LOG_ERROR("missing required field max_traces_per_minute in rule. Skipping the rule",
                {"ShortDebugString", throttlingRule.ShortDebugString()});
            continue;
        }
        if (throttlingRule.GetMaxTracesPerMinute() == 0) {
            YDB_LOG_ERROR("max_traces_per_minute should never be zero. Found in rule. Skipping the rule",
                {"GetMaxTracesPerMinute", throttlingRule.GetMaxTracesPerMinute()});
            continue;
        }

        ui64 maxRatePerMinute = throttlingRule.GetMaxTracesPerMinute();
        ui64 maxBurst = throttlingRule.GetMaxTracesBurst();
        TExternalThrottlingRule<TWithTag<TThrottlingSettings>> rule {
            .Level = static_cast<ui8>(level),
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

IActor* CreateJaegerTracingConfigurator(TIntrusivePtr<TSamplingThrottlingConfigurator> tracingConfigurator,
                                        NKikimrConfig::TTracingConfig cfg) {
    return new TJaegerTracingConfigurator(std::move(tracingConfigurator), std::move(cfg));
}

} // namespace NKikimr::NConsole
