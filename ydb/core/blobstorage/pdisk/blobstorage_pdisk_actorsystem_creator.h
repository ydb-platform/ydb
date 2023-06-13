#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/services.pb.h>

#include <ydb/library/pdisk_io/aio.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/executor_pool_io.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/actors/core/scheduler_basic.h>

namespace NKikimr {

class TActorSystemCreator {
    std::unique_ptr<TAppData> AppData;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    std::unique_ptr<NActors::TActorSystem> ActorSystem;

public:
    TActorSystemCreator()
    {
        using namespace NActors;

        AppData = std::make_unique<TAppData>(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        AppData->IoContextFactory = IoContext.get();

        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 3;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[3]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

        auto logSettings = MakeIntrusive<NActors::NLog::TSettings>(NActors::TActorId(1, "logger"),
                NKikimrServices::LOGGER, NActors::NLog::PRI_ERROR, NActors::NLog::PRI_ERROR, ui32{0});
        logSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );
        logSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto loggerActor = std::make_unique<NActors::TLoggerActor>(logSettings, NActors::CreateNullBackend(),
                GetServiceCounters(Counters, "utils"));
        NActors::TActorSetupCmd loggerActorCmd(std::move(loggerActor), NActors::TMailboxType::Simple, 2);
        setup->LocalServices.emplace_back(NActors::TActorId(1, "logger"), std::move(loggerActorCmd));

        ActorSystem = std::make_unique<TActorSystem>(setup, AppData.get(), logSettings);
        ActorSystem->Start();
    }

    TActorSystem *GetActorSystem() {
        return ActorSystem.get();
    }
};

} // NKikimr
