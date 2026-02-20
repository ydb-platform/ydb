#pragma once

#include "defs.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tabletid.h>

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_crypto.h>

#include <ydb/core/load_test/service_actor.h>

#include <ydb/core/tablet/bootstrapper.h>

#include <ydb/core/protos/blobstorage.pb.h>
#if __has_include("ydb/core/protos/blobstorage.deps.pb.h")
    #include <ydb/core/protos/blobstorage.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/core/protos/blobstorage_pdisk_config.pb.h>
#if __has_include("ydb/core/protos/blobstorage_pdisk_config.deps.pb.h")
    #include <ydb/core/protos/blobstorage_pdisk_config.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>
#if __has_include("ydb/core/protos/blobstorage_vdisk_config.deps.pb.h")
    #include <ydb/core/protos/blobstorage_vdisk_config.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>
#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/string/printf.h>
#include <util/system/event.h>

#include "device_test_tool.h"

#ifdef _linux_
#   include <sys/ioctl.h>
#endif


namespace NKikimr {

static TSystemEvent DoneEvent(TSystemEvent::rAuto);
static TSystemEvent ResultsPrintedEvent(TSystemEvent::rAuto);

#define ASSERT_YTHROW(expr, str) \
do { \
    if (!(expr)) { \
        ythrow TWithBackTrace<yexception>() << str; \
    } \
} while(false)



class TPerfTestActor : public TActor<TPerfTestActor> {
    const TActorId Yard;
    const TVDiskID VDiskID;
    ui32 TestStep = 0;
    ui64 CurrentTest = 0;
    const TPerfTestConfig& Cfg;
    const NDevicePerfTest::TPDiskTest& TestProto;
    TIntrusivePtr<IResultPrinter> Printer;
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& Counters;
    TString CurrentTestType; // Current test type name (e.g., "PDiskWriteLoad")


protected:
    void ActTestFSM(const TActorContext& ctx) {
        switch (TestStep) {
        case 0:
            if (CurrentTest < TestProto.PDiskTestListSize()) {
                auto record = TestProto.GetPDiskTestList(CurrentTest);
                switch(record.Command_case()) {
                case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskReadLoad: {
                    CurrentTestType = "PDiskReadLoad";
                    const auto cfg = record.GetPDiskReadLoad();
                    ctx.Register(CreatePDiskReaderLoadTest(cfg, ctx.SelfID, Counters,
                                CurrentTest, cfg.HasTag() ? cfg.GetTag() : 0));
                    break;
                }
                case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskWriteLoad: {
                    CurrentTestType = "PDiskWriteLoad";
                    const auto cfg = record.GetPDiskWriteLoad();
                    ctx.Register(CreatePDiskWriterLoadTest(cfg, ctx.SelfID, Counters,
                                CurrentTest, cfg.HasTag() ? cfg.GetTag() : 0));
                    break;
                }
                case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskLogLoad: {
                    CurrentTestType = "PDiskLogLoad";
                    const auto cfg = record.GetPDiskLogLoad();
                    ctx.Register(CreatePDiskLogWriterLoadTest(cfg, ctx.SelfID, Counters,
                                CurrentTest, cfg.HasTag() ? cfg.GetTag() : 0));
                    break;
                }
                default:
                    CurrentTestType = "Unknown";
                    Cerr << "Unknown load type" << Endl;
                    break;
                }
                ++CurrentTest;
            }
            if (CurrentTest == TestProto.PDiskTestListSize()) {
                TestStep += 10;
            }
            break;
        case 10:
            break;
        }
    }

    void HandleBoot(TEvTablet::TEvBoot::TPtr& ev, const TActorContext& ctx) {
        ASSERT_YTHROW(TestStep == 0, "Error in messages order");
        ActTestFSM(ctx);
        Y_UNUSED(ev);
    }

    /*
    void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        ASSERT_YTHROW(TestStep == 10, "Error in messages order");
        ActTestFSM(ctx);
        Y_UNUSED(ev);
    }
    */

    void Handle(TEvLoad::TEvLoadTestFinished::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ev);
        Y_ABORT_UNLESS(ev->Get());
        TIntrusivePtr<TEvLoad::TLoadReport> report = ev->Get()->Report;
        if (report) {
            double speedMBps = report->GetAverageSpeed() / 1e6;
            double iops = report->Size ? (report->GetAverageSpeed() / report->Size) : 0.0;
            Printer->SetTestType(CurrentTestType);
            Printer->SetInFlight(report->InFlight);
            Printer->AddResult("Name", Cfg.Name);
            Printer->AddResult("Duration, sec", report->Duration.Seconds());
            Printer->AddResult("Load", report->LoadTypeName());
            Printer->AddResult("Size", ToString(HumanReadableSize(report->Size, SF_BYTES)));
            Printer->AddResult("InFlight", report->InFlight);
            Printer->AddResult("Speed", Sprintf("%.1f MB/s", speedMBps));
            if (report->Size) {
                Printer->AddResult("IOPS", Sprintf("%.0f", iops));
            } else {
                Printer->AddResult("IOPS", TString("N/A"));
            }
            Printer->AddSpeedAndIops(TSpeedAndIops(speedMBps, iops));
            for (double perc : {1.0, 0.9999, 0.999, 0.99, 0.95, 0.9, 0.5, 0.1}) {
                TString perc_name = Sprintf("p%.2f", perc * 100);
                size_t val = report->LatencyUs.GetPercentile(perc);
                Printer->AddResult(perc_name, Sprintf("%zu us", val));
            }
        } else {
            Cerr << "Test error - no report on test finish, reason# " << ev->Get()->ErrorReason;
        }

        ActTestFSM(ctx);
        DoneEvent.Signal();
        ResultsPrintedEvent.Wait();
    }

public:
    TPerfTestActor(const TActorId yard, const TVDiskID vDiskID, const TPerfTestConfig& perfCfg,
            const NDevicePerfTest::TPDiskTest& testProto, const TIntrusivePtr<IResultPrinter>& printer,
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
        : TActor(&TThis::StateRegister)
        , Yard(yard)
        , VDiskID(vDiskID)
        , Cfg(perfCfg)
        , TestProto(testProto)
        , Printer(printer)
        , Counters(counters)
    {}

    STFUNC(StateRegister) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLoad::TEvLoadTestFinished, Handle);
            HFunc(TEvTablet::TEvBoot, HandleBoot);
        }
    }
};


template<ui32 ChunkSize = 128 << 20 >
struct TPDiskTest : public TPerfTest {
    THolder<TActorSystemSetup> Setup;
    TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
    TActorId PDiskId;
    THolder<TActorSystem> ActorSystem;
    TAppData AppData;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    yexception LastException;
    volatile bool IsLastExceptionSet = false;
    const ui64 PDiskGuid = 12345;
    TActorId TestId;
    const NDevicePerfTest::TPDiskTest& TestProto;

    TDuration InitialSleep = TDuration::Seconds(10);

    TPDiskTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TPDiskTest& testProto)
        : TPerfTest(cfg)
        , Setup(new TActorSystemSetup())
        , LogSettings(new NActors::NLog::TSettings(NActors::TActorId(1, "logger"),
                                                   NActorsServices::LOGGER,
                                                   NActors::NLog::PRI_ERROR,
                                                   NActors::NLog::PRI_ERROR,
                                                   0))
        , PDiskId(MakeBlobStoragePDiskID(1, 1))
        , AppData(0 // sysPoolId
                , 1 // userPoolid
                , 3 // ioPoolId
                , 2 // batchPoolId
                , TMap<TString, ui32>() // servicePools
                , nullptr // typeRegistry
                , nullptr // functionRegistry
                , nullptr // formatFactory
                , nullptr) // kikimrShouldContinue
        , IoContext(std::make_shared<NPDisk::TIoContextFactoryOSS>())
        , TestProto(testProto)
    {
         AppData.IoContextFactory = IoContext.get();
    }

    void FormatPDiskForTest() {
        NPDisk::TKey chunkKey;
        NPDisk::TKey logKey;
        NPDisk::TKey sysLogKey;
        EntropyPool().Read(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
        EntropyPool().Read(&logKey, sizeof(NKikimr::NPDisk::TKey));
        EntropyPool().Read(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));
        ui64 diskSize = 0;
        bool isErasureEncode = false;
        TFormatOptions options;
        options.IsErasureEncodeUserLog = isErasureEncode;
        if (Cfg.SectorMap) {
            options.SectorMap = Cfg.SectorMap;
            options.EnableSmallDiskOptimization = false;
            diskSize = Cfg.SectorMap->DeviceSize;
        }
        FormatPDisk(Cfg.Path, diskSize, 4 << 10, ChunkSize, PDiskGuid,
            chunkKey, logKey, sysLogKey, NPDisk::YdbDefaultPDiskSequence, "Info", options);
    }

    void DoBasicSetup() {
        Counters = TIntrusivePtr<NMonitoring::TDynamicCounters>(new NMonitoring::TDynamicCounters());

        TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());

        Setup->NodeId = 1;
        Setup->ExecutorsCount = 4;
        Setup->Executors.Reset(new TAutoPtr<IExecutorPool>[4]);
        Setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20, "nameservice"));
        Setup->Executors[1].Reset(new TBasicExecutorPool(1, 1, 20, "pdisk_actors"));
        Setup->Executors[2].Reset(new TBasicExecutorPool(2, 4, 20, "perf_actors"));
        Setup->Executors[3].Reset(new TIOExecutorPool(3, 1, "IO"));
        Setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(64, 20)));

        const TActorId nameserviceId = GetNameserviceActorId();
        TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
        Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(nameserviceId, std::move(nameserviceSetup)));

        // PDisk
        FormatPDiskForTest();

        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(Cfg.Path, PDiskGuid, 1, TPDiskCategory(Cfg.DeviceType, 0).GetRaw());
        pDiskConfig->DriveModelSeekTimeNs = 1000ull;
        pDiskConfig->DriveModelSpeedBps = 1 << 30;
        pDiskConfig->DriveModelSpeedBpsMin = 1 << 30;
        pDiskConfig->DriveModelSpeedBpsMax = 1 << 30;
        pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->ChunkSize = ChunkSize;
        pDiskConfig->DeviceInFlight = TestProto.GetDeviceInFlight() != 0 ? FastClp2(TestProto.GetDeviceInFlight()) : 4;
        pDiskConfig->UseNoopScheduler = true;
        pDiskConfig->FeatureFlags.SetEnableSeparateSubmitThreadForPDisk(true);
        pDiskConfig->FeatureFlags.SetEnablePDiskDataEncryption(!Cfg.DisablePDiskDataEncryption);
        if (Cfg.SectorMap) {
            pDiskConfig->SectorMap = Cfg.SectorMap;
        }
        if (!TestProto.GetEnableTrim()) {
            pDiskConfig->DriveModelTrimSpeedBps = 0;
        }

        if (pDiskConfig->DriveModelTrimSpeedBps > 0) {
            Printer->AddGlobalParam("Trim", "on");
            Printer->AddGlobalParam("TrimSpeedBps", pDiskConfig->DriveModelTrimSpeedBps);
        } else {
            Printer->AddGlobalParam("Trim", "off");
        }
        Printer->AddGlobalParam("PDiskInFlight", pDiskConfig->DeviceInFlight);
#if ENABLE_PDISK_ENCRYPTION
        Printer->AddGlobalParam("Encryption", "on");
#else
        if (Cfg.DisablePDiskDataEncryption) {
            Printer->AddGlobalParam("Encryption", "off");
        } else {
            Printer->AddGlobalParam("Encryption", "on");
        }
#endif

        TActorSetupCmd pDiskSetup(CreatePDisk(pDiskConfig.Get(),
                    NPDisk::TMainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence }, .IsInitialized = true }, Counters),
                    TMailboxType::ReadAsFilled, 1);
        Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(PDiskId, std::move(pDiskSetup)));

        /////////////////////// LOGGER ///////////////////////////////////////////////

        LogSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );
        LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );

        TString explanation;
        LogSettings->SetLevel(NLog::PRI_EMERG, NKikimrServices::BS_DEVICE, explanation);
        LogSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_LOAD_TEST, explanation);
        LogSettings->SetLevel(NLog::PRI_ERROR, NKikimrServices::BS_PDISK, explanation);

        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(LogSettings, NActors::CreateStderrBackend(),
            GetServiceCounters(Counters, "utils"));
        NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 3);
        std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(NActors::TActorId(1, "logger"),
            std::move(loggerActorCmd));
        Setup->LocalServices.push_back(std::move(loggerActorPair));
    }

    virtual void SetupLoadActor() {
        TActorId yardId = PDiskId;
        TestId = MakeBlobStorageProxyID(1);
        TActorSetupCmd testSetup(new TPerfTestActor(yardId, TVDiskID(0, 1, 0, 0, 0), Cfg, TestProto, Printer, Counters),
            TMailboxType::ReadAsFilled, 2);
        Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(TestId, std::move(testSetup)));
    }

    void StartActorSystem() {
        ActorSystem.Reset(new TActorSystem(Setup, &AppData, LogSettings));
        ActorSystem->Start();
        Sleep(InitialSleep);
    }

    void Init() override {
        try {
            DoBasicSetup();
            SetupLoadActor();
            StartActorSystem();
        } catch (yexception& ex) {
            IsLastExceptionSet = true;
            VERBOSE_COUT("Error on init state, what# " << ex.what());
        }
    }

    void Run() override {
        if (IsLastExceptionSet) {
            return;
        }

        try {
            ActorSystem->Send(TestId, new TEvTablet::TEvBoot(MakeTabletID(0, 0, 1), 0, nullptr, TActorId(), nullptr));

            //TInstant startTime = Now();
            for (ui32 i = 0; i < TestProto.PDiskTestListSize(); ++i) {
                DoneEvent.Wait();
                Printer->PrintResults();
                ResultsPrintedEvent.Signal();
            }
        } catch (yexception ex) {
            LastException = ex;
            IsLastExceptionSet = true;
            VERBOSE_COUT(ex.what());
        }

        if (ActorSystem.Get()) {
            ActorSystem->Stop();
            ActorSystem.Destroy();
        }
        DoneEvent.Reset();
        if (IsLastExceptionSet) {
            IsLastExceptionSet = false;
            ythrow LastException;
        }
    }

    void Finish() override {
    }

    ~TPDiskTest() override {
    }
};

} // namespace NKikimr
