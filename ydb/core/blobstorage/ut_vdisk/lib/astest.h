#pragma once

#include "defs.h"
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/library/pdisk_io/aio.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/system/event.h>


////////////////////////////////////////////////////////////////////
// TTestWithActorSystem
////////////////////////////////////////////////////////////////////
class TTestWithActorSystem {
public:
    TTestWithActorSystem(ui16 monPort = 8088)
        : MonPort(monPort) // 0 - select random port, otherwise use specified port
    {}
    void Run(NActors::IActor *testActor);
    void Signal() {
        DoneEvent.Signal();
    }

private:
    TProgramShouldContinue KikimrShouldContinue;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    std::unique_ptr<NActors::TMon> Monitoring;
    std::unique_ptr<NKikimr::TAppData> AppData;
    std::shared_ptr<NKikimr::NPDisk::IIoContextFactory> IoContext;
    std::unique_ptr<NActors::TActorSystem> ActorSystem1;
    TSystemEvent DoneEvent { TSystemEvent::rAuto };
public:
    ui16 MonPort;
};


inline void TTestWithActorSystem::Run(NActors::IActor *testActor) {
    using namespace NActors;
    Counters = TIntrusivePtr<::NMonitoring::TDynamicCounters>(new ::NMonitoring::TDynamicCounters());
    TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
    TPortManager pm;
    nameserverTable->StaticNodeTable[1] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12001));
    nameserverTable->StaticNodeTable[2] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12002));

    auto setup1 = MakeHolder<TActorSystemSetup>();
    setup1->NodeId = 1;
    setup1->ExecutorsCount = 4;
    setup1->Executors.Reset(new TAutoPtr<IExecutorPool>[4]);
    setup1->Executors[0].Reset(new TBasicExecutorPool(0, 8, 20));
    setup1->Executors[1].Reset(new TBasicExecutorPool(1, 8, 20));
    setup1->Executors[2].Reset(new TIOExecutorPool(2, 10));
    setup1->Executors[3].Reset(new TBasicExecutorPool(3, 8, 20));
    setup1->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

    const TActorId nameserviceId = GetNameserviceActorId();
    TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
    setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(nameserviceId, std::move(nameserviceSetup)));


    ///////////////////////// LOGGER ///////////////////////////////////////////////
    NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
    TIntrusivePtr<NActors::NLog::TSettings> logSettings;
    logSettings.Reset(new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER, NActors::NLog::PRI_ERROR,
                                                   NActors::NLog::PRI_DEBUG, 0)); // NOTICE
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
    TString explanation;
    //logSettings->SetLevel(NLog::PRI_INFO, NKikimrServices::BS_SKELETON, explanation);
    //logSettings->SetLevel(NLog::PRI_INFO, NKikimrServices::BS_HULLCOMP, explan
    NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(logSettings,
                                                                   NActors::CreateStderrBackend(),
                                                                   Counters->GetSubgroup("logger", "counters"));
    NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 0);
    std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(loggerActorId, std::move(loggerActorCmd));
    setup1->LocalServices.push_back(std::move(loggerActorPair));
    //////////////////////////////////////////////////////////////////////////////

    ///////////////////////// SETUP TEST ACTOR ///////////////////////////////////
    NActors::TActorId testActorId = NActors::TActorId(1, "test123");
    TActorSetupCmd testActorSetup(testActor, TMailboxType::Simple, 0);
    setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(testActorId, std::move(testActorSetup)));
    //////////////////////////////////////////////////////////////////////////////

    ///////////////////////// TYPE REGISTRY //////////////////////////////////////
    TIntrusivePtr<NKikimr::NScheme::TTypeRegistry> typeRegistry(new NKikimr::NScheme::TKikimrTypeRegistry());
    //////////////////////////////////////////////////////////////////////////////

    ///////////////////////// MONITORING SETTINGS /////////////////////////////////
    if (!MonPort) {
        MonPort = pm.GetPort(MonPort);
    }
    Monitoring.reset(new NActors::TSyncHttpMon({
        .Port = MonPort,
        .Title = "at"
    }));
    NMonitoring::TIndexMonPage *actorsMonPage = Monitoring->RegisterIndexPage("actors", "Actors");
    Y_UNUSED(actorsMonPage);
    Monitoring->RegisterCountersPage("counters", "Counters", Counters);
    Monitoring->Start();
    loggerActor->Log(Now(), NKikimr::NLog::PRI_NOTICE, NActorsServices::TEST, "Monitoring settings set up");
    //////////////////////////////////////////////////////////////////////////////

    AppData.reset(new NKikimr::TAppData(0, 1, 2, 3, TMap<TString, ui32>(), typeRegistry.Get(),
                                        nullptr, nullptr, &KikimrShouldContinue));
    AppData->Counters = Counters;
    AppData->Mon = Monitoring.get();
    IoContext = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
    AppData->IoContextFactory = IoContext.get();
    ActorSystem1.reset(new TActorSystem(setup1, AppData.get(), logSettings));
    loggerActor->Log(Now(), NKikimr::NLog::PRI_NOTICE, NActorsServices::TEST, "Actor system created");


    ActorSystem1->Start();
    LOG_NOTICE(*ActorSystem1, NActorsServices::TEST, "Actor system started");



    DoneEvent.Wait();
    ActorSystem1->Stop();
    ActorSystem1.reset();
}
