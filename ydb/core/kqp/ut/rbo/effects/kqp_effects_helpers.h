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

        auto config = settings.AppConfig.GetTableServiceConfig();
        config.SetEnableNewRBO(true);
        config.SetEnableFallbackToYqlOptimizer(false);

        // Fixture queries have finished. Switch every compile service before
        // the test can issue queries, waiting for each configuration update.
        auto& runtime = *GetTestServer().GetRuntime();
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            const auto edge = runtime.AllocateEdgeActor(nodeIndex);
            auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            *notification->Record.MutableConfig()->MutableTableServiceConfig() = config;
            runtime.Send(new IEventHandle(MakeKqpCompileServiceID(runtime.GetNodeId(nodeIndex)), edge,
                notification.Release()), nodeIndex);
            UNIT_ASSERT(runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
                edge, TDuration::Seconds(30)));
        }
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
