#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/log_settings.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

using namespace NActors;

namespace NYql {

    // TActorSystemManager is an utility class helping to initialize actor system
    class TActorSystemManager {
    public:
        TActorSystemManager(
            IMetricsRegistryPtr& metricsRegistry,
            NActors::NLog::EPriority loggingLevel,
            TIntrusivePtr<NActors::NLog::TSettings> loggingSettings = nullptr);

        using TSetupModifier = std::function<void(TActorSystemSetup& setup)>;

        // Changes actor system settings with callable object
        void ApplySetupModifier(const TSetupModifier& modifier);

        // Runs actors system. Can be called only once.
        // It's not possible to call ApplySetupModifier after Start has been called.
        void Start();

        // This getter violates incapsulaton, but various functions among YDB/YQL code really need
        // direct access to the actor system.
        // Call it only after Start() method has been called.
        TActorSystem* GetActorSystem();

        ~TActorSystemManager();

    private:
        // Copied from https://a.yandex-team.ru/arcadia/yql/providers/dq/service/interconnect_helpers.cpp?rev=r11761215
        class TYqlLogBackend: public TLogBackend {
            void WriteData(const TLogRecord& rec) override {
                TString message(rec.Data, rec.Len);
                if (message.find("ICP01 ready to work") != TString::npos) {
                    return;
                }
                YQL_CLOG(DEBUG, ProviderDq) << message;
            }

            void ReopenLog() override {
            }
        };

        // Provides some reasonable defaults for actor system settings
        static void ModifySetupWithDefaults(TActorSystemSetup& setup, ui32 nodeId);

        // Used for ActorId initialization
        ui32 ObtainNextActorNodeId();

        // Actor system
        THolder<TActorSystemSetup> Setup_;
        std::optional<TActorSystem> ActorSystem_;
        ui32 ActorNodeIDCounter_;

        // Logging infrastructure
        TIntrusivePtr<NActors::NLog::TSettings> LoggingSettings_;
        THolder<TLogBackend> LoggerBackend_ = nullptr;
        IMetricsRegistryPtr MetricsRegistry_;
    };
}
