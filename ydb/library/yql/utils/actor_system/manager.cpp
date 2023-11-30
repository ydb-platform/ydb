#include "manager.h"

using namespace NActors;

namespace NYql {
    TActorSystemManager::TActorSystemManager(
        IMetricsRegistryPtr& metricsRegistry,
        NActors::NLog::EPriority loggingLevel,
        TIntrusivePtr<NActors::NLog::TSettings> loggingSettings)
        : Setup_(MakeHolder<TActorSystemSetup>())
        , ActorSystem_(std::nullopt)
        , ActorNodeIDCounter_(0)
        , LoggingSettings_(loggingSettings)
        , MetricsRegistry_(metricsRegistry)
    {
        // Enable default logging
        if (LoggingSettings_ == nullptr) {
            NActors::TActorId loggerActorId(ObtainNextActorNodeId(), "logger");
            LoggingSettings_ = MakeIntrusive<NActors::NLog::TSettings>(loggerActorId, 0, loggingLevel);
            LoggingSettings_->Append(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                [](NActors::NLog::EComponent component) -> const TString& { return NKikimrServices::EServiceKikimr_Name(component); });
        }
    }

    void TActorSystemManager::ApplySetupModifier(const TSetupModifier& modifier) {
        YQL_ENSURE(!ActorSystem_.has_value());
        modifier(*Setup_);
    }

    void TActorSystemManager::Start() {
        YQL_ENSURE(!ActorSystem_.has_value());

        // Initialize actor system setup with reasonable defaults
        ApplySetupModifier([&](TActorSystemSetup& setup) { ModifySetupWithDefaults(setup, ObtainNextActorNodeId()); });

        // Create actor for logging
        LoggerBackend_ = MakeHolder<TYqlLogBackend>();
        TLoggerActor* loggerActor = new NActors::TLoggerActor(
            LoggingSettings_,
            LoggerBackend_,
            MetricsRegistry_->GetSensors()->GetSubgroup("logger", "counters"));
        Setup_->LocalServices.emplace_back(LoggingSettings_->LoggerActorId, TActorSetupCmd(loggerActor, TMailboxType::Simple, 0));

        // Initialize actor system instance and start
        ActorSystem_.emplace(Setup_, nullptr, LoggingSettings_);
        ActorSystem_->Start();
    }

    TActorSystem* TActorSystemManager::GetActorSystem() {
        YQL_ENSURE(ActorSystem_.has_value());

        return std::addressof(ActorSystem_.value());
    }

    TActorSystemManager::~TActorSystemManager() {
        if (ActorSystem_.has_value()) {
            ActorSystem_->Stop();
            ActorSystem_->Cleanup();
            ActorSystem_.reset();
        }
    }

    // Inspired by simple example from
    // https://a.yandex-team.ru/arcadia/ydb/library/actors/examples/01_ping_pong/main.cpp?rev=r11692799
    void TActorSystemManager::ModifySetupWithDefaults(TActorSystemSetup& setup, ui32 nodeID) {
        if (setup.NodeId == 0) {
            setup.NodeId = nodeID;
        }

        if (setup.ExecutorsCount == 0) {
            const ui32 pools = 1;
            const ui32 threads = 2;
            const ui64 spinThreshold = 50;

            setup.ExecutorsCount = pools;
            setup.Executors.Reset(new TAutoPtr<IExecutorPool>[pools]);
            for (ui32 idx : xrange(pools)) {
                setup.Executors[idx] = new TBasicExecutorPool(idx, threads, spinThreshold);
            }
        }

        if (setup.Scheduler.Get() == 0) {
            const ui64 resolution = 512;
            const ui64 spinThreshold = 0;
            setup.Scheduler = new TBasicSchedulerThread(TSchedulerConfig(resolution, spinThreshold));
        }
    }

    ui32 TActorSystemManager::ObtainNextActorNodeId() {
        return ActorNodeIDCounter_++;
    }
}
