#pragma once

#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

inline NKikimrConfig::TAppConfig NewRboAppConfig() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    return appConfig;
}

inline void EnableNewRbo(TKikimrRunner& kikimr, NKikimrConfig::TTableServiceConfig config) {
    config.SetEnableNewRBO(true);
    config.SetEnableFallbackToYqlOptimizer(false);

    // Call only after schema and fixture queries have finished. Preserve the
    // test's other settings and wait for every compile service and proxy to
    // acknowledge strict RBO. The proxy supplies the optimizer setting to new
    // execution sessions as well (notably for execution-plan statistics).
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        const auto edge = runtime.AllocateEdgeActor(nodeIndex);
        for (const auto service : {MakeKqpCompileServiceID(runtime.GetNodeId(nodeIndex)), MakeKqpProxyID(runtime.GetNodeId(nodeIndex))}) {
            auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            *notification->Record.MutableConfig() = *kikimr.GetTestServer().GetSettings().AppConfig;
            *notification->Record.MutableConfig()->MutableTableServiceConfig() = config;
            runtime.Send(new IEventHandle(service, edge, notification.Release()), nodeIndex);
            UNIT_ASSERT(runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
                edge, TDuration::Seconds(30)));
        }
    }
}

inline void EnableNewRbo(TKikimrRunner& kikimr) {
    EnableNewRbo(kikimr, kikimr.GetTestServer().GetSettings().AppConfig->GetTableServiceConfig());
}

class TEffectsKikimrRunner : public TKikimrRunner {
public:
    explicit TEffectsKikimrRunner(const TKikimrSettings& settings)
        : TKikimrRunner(FixtureSettings(settings))
    {
        // Without sample tables, start with strict RBO enabled. There is no
        // fixture work to wait for or compile-service configuration to switch.
        if (!settings.WithSampleTables) {
            return;
        }

        EnableNewRbo(*this, settings.AppConfig.GetTableServiceConfig());
    }

    explicit TEffectsKikimrRunner(const NKikimrConfig::TAppConfig& appConfig)
        : TEffectsKikimrRunner(TKikimrSettings(appConfig))
    {}

private:
    static TKikimrSettings FixtureSettings(TKikimrSettings settings) {
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(!settings.WithSampleTables);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        return settings;
    }
};

inline TEffectsKikimrRunner NewRboKikimrRunner(TVector<NKikimrKqp::TKqpSetting> kqpSettings = {}) {
    return TEffectsKikimrRunner(TKikimrSettings(NewRboAppConfig())
        .SetKqpSettings(kqpSettings)
        .SetEnableScriptExecutionOperations(true));
}

} // namespace NKikimr::NKqp
