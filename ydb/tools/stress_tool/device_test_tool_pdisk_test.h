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

struct TPDiskDeviceInfo {
    ui32 PDiskIdNum;
    ui64 PDiskGuid;
};

class TPerfTestActor : public TActor<TPerfTestActor> {
    TVector<TPDiskDeviceInfo> Devices;
    ui64 CurrentTest = 0;
    const TPerfTestConfig& Cfg;
    const NDevicePerfTest::TPDiskTest& TestProto;
    TIntrusivePtr<IResultPrinter> Printer;
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& Counters;
    TString CurrentTestType;

    struct TDeviceResult {
        TIntrusivePtr<TEvLoad::TLoadReport> Report;
        TString ErrorReason;
    };
    TVector<TDeviceResult> PendingResults;
    ui32 ReceivedResults = 0;

protected:
    void FillPrinterWithReport(const TIntrusivePtr<TEvLoad::TLoadReport>& report, i32 deviceIdx) {
        double speedMBps = report->GetAverageSpeed() / 1e6;
        double iops = report->Size ? (report->GetAverageSpeed() / report->Size) : 0.0;
        Printer->SetTestType(CurrentTestType);
        Printer->SetInFlight(report->InFlight);
        if (Devices.size() > 1) {
            Printer->AddResult("Device", deviceIdx);
        }
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
    }

    void FillPrinterWithAggregate() {
        double totalSpeed = 0.0;
        double totalIops = 0.0;
        NMonitoring::TPercentileTrackerLg<10, 4, 1> mergedLatency;
        for (const auto& r : PendingResults) {
            if (r.Report) {
                totalSpeed += r.Report->GetAverageSpeed() / 1e6;
                totalIops += r.Report->Size ? (r.Report->GetAverageSpeed() / r.Report->Size) : 0.0;
                for (size_t i = 0; i < mergedLatency.ITEMS_COUNT; ++i) {
                    mergedLatency.Items[i].fetch_add(
                        r.Report->LatencyUs.Items[i].load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
                }
            }
        }
        const auto& firstReport = !PendingResults.empty() ? PendingResults[0].Report : nullptr;
        Printer->SetTestType(CurrentTestType);
        if (firstReport) {
            Printer->SetInFlight(firstReport->InFlight);
        }
        Printer->AddResult("Device", TString("SUM"));
        Printer->AddResult("Name", Cfg.Name);
        Printer->AddResult("Duration, sec", firstReport ? firstReport->Duration.Seconds() : 0);
        Printer->AddResult("Load", firstReport ? firstReport->LoadTypeName() : TString("-"));
        Printer->AddResult("Size", firstReport ? ToString(HumanReadableSize(firstReport->Size, SF_BYTES)) : TString("-"));
        Printer->AddResult("InFlight", firstReport ? firstReport->InFlight : 0u);
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", totalSpeed));
        Printer->AddResult("IOPS", Sprintf("%.0f", totalIops));
        Printer->AddSpeedAndIops(TSpeedAndIops(totalSpeed, totalIops));
        for (double perc : {1.0, 0.9999, 0.999, 0.99, 0.95, 0.9, 0.5, 0.1}) {
            TString perc_name = Sprintf("p%.2f", perc * 100);
            size_t val = mergedLatency.GetPercentile(perc);
            Printer->AddResult(perc_name, Sprintf("%zu us", val));
        }
    }

    void LaunchTestOnAllDevices(const TActorContext& ctx) {
        auto baseRecord = TestProto.GetPDiskTestList(CurrentTest);

        switch (baseRecord.Command_case()) {
        case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskReadLoad:
            CurrentTestType = "PDiskReadLoad";
            break;
        case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskWriteLoad:
            CurrentTestType = "PDiskWriteLoad";
            break;
        case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskLogLoad:
            CurrentTestType = "PDiskLogLoad";
            break;
        default:
            CurrentTestType = "Unknown";
            Cerr << "Unknown load type" << Endl;
            return;
        }

        PendingResults.clear();
        PendingResults.resize(Devices.size());
        ReceivedResults = 0;

        for (ui32 d = 0; d < Devices.size(); ++d) {
            auto record = baseRecord;
            switch (record.Command_case()) {
            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskWriteLoad: {
                auto* cfg = record.MutablePDiskWriteLoad();
                cfg->SetPDiskId(Devices[d].PDiskIdNum);
                cfg->SetPDiskGuid(Devices[d].PDiskGuid);
                cfg->MutableVDiskId()->SetRing(0);
                cfg->MutableVDiskId()->SetDomain(0);
                cfg->MutableVDiskId()->SetVDisk(d);
                cfg->SetIsWardenlessTest(true);
                ctx.Register(CreatePDiskWriterLoadTest(record.GetPDiskWriteLoad(), ctx.SelfID, Counters, d, d));
                break;
            }
            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskReadLoad: {
                auto* cfg = record.MutablePDiskReadLoad();
                cfg->SetPDiskId(Devices[d].PDiskIdNum);
                cfg->SetPDiskGuid(Devices[d].PDiskGuid);
                cfg->MutableVDiskId()->SetRing(0);
                cfg->MutableVDiskId()->SetDomain(0);
                cfg->MutableVDiskId()->SetVDisk(d);
                cfg->SetIsWardenlessTest(true);
                ctx.Register(CreatePDiskReaderLoadTest(record.GetPDiskReadLoad(), ctx.SelfID, Counters, d, d));
                break;
            }
            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskLogLoad: {
                auto* cfg = record.MutablePDiskLogLoad();
                cfg->SetPDiskId(Devices[d].PDiskIdNum);
                cfg->SetPDiskGuid(Devices[d].PDiskGuid);
                cfg->SetIsWardenlessTest(true);
                for (size_t w = 0; w < cfg->WorkersSize(); ++w) {
                    auto* vdiskId = cfg->MutableWorkers(w)->MutableVDiskId();
                    vdiskId->SetRing(0);
                    vdiskId->SetDomain(0);
                    vdiskId->SetVDisk(d);
                }
                ctx.Register(CreatePDiskLogWriterLoadTest(record.GetPDiskLogLoad(), ctx.SelfID, Counters, d, d));
                break;
            }
            default:
                break;
            }
        }
    }

    void ProcessAllResults() {
        if (Devices.size() == 1) {
            if (PendingResults[0].Report) {
                FillPrinterWithReport(PendingResults[0].Report, 0);
            } else {
                Cerr << "Test error - no report on test finish, reason# " << PendingResults[0].ErrorReason << Endl;
            }
            DoneEvent.Signal();
            ResultsPrintedEvent.Wait();
        } else {
            Printer->SetSkipStatistics(true);
            for (ui32 d = 0; d < Devices.size(); ++d) {
                if (PendingResults[d].Report) {
                    FillPrinterWithReport(PendingResults[d].Report, d);
                } else {
                    Cerr << "Test error on device " << d << " - no report, reason# " << PendingResults[d].ErrorReason << Endl;
                }
                DoneEvent.Signal();
                ResultsPrintedEvent.Wait();
            }
            Printer->SetSkipStatistics(false);
            FillPrinterWithAggregate();
            DoneEvent.Signal();
            ResultsPrintedEvent.Wait();
        }
    }

    void HandleBoot(TEvTablet::TEvBoot::TPtr& ev, const TActorContext& ctx) {
        if (CurrentTest < TestProto.PDiskTestListSize()) {
            LaunchTestOnAllDevices(ctx);
        }
        Y_UNUSED(ev);
    }

    void Handle(TEvLoad::TEvLoadTestFinished::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ev);
        Y_ABORT_UNLESS(ev->Get());

        ui32 deviceIdx = static_cast<ui32>(ev->Get()->Tag);
        Y_ABORT_UNLESS(deviceIdx < Devices.size());
        PendingResults[deviceIdx] = {ev->Get()->Report, ev->Get()->ErrorReason};
        ++ReceivedResults;

        if (ReceivedResults == Devices.size()) {
            ProcessAllResults();
            ReceivedResults = 0;
            ++CurrentTest;
            if (CurrentTest < TestProto.PDiskTestListSize()) {
                LaunchTestOnAllDevices(ctx);
            }
        }
    }

public:
    TPerfTestActor(const TVector<TPDiskDeviceInfo>& devices, const TPerfTestConfig& perfCfg,
            const NDevicePerfTest::TPDiskTest& testProto, const TIntrusivePtr<IResultPrinter>& printer,
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
        : TActor(&TThis::StateRegister)
        , Devices(devices)
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
    THolder<TActorSystem> ActorSystem;
    TAppData AppData;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    yexception LastException;
    volatile bool IsLastExceptionSet = false;
    TActorId TestId;
    const NDevicePerfTest::TPDiskTest& TestProto;

    TVector<TActorId> PDiskActorIds;
    TVector<ui64> PDiskGuids;

    // Backward-compatible aliases for first device (used by TDDiskTest, TPersistentBufferTest)
    TActorId PDiskId;
    ui64 PDiskGuid;

    TDuration InitialSleep = TDuration::Seconds(10);

    TPDiskTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TPDiskTest& testProto)
        : TPerfTest(cfg)
        , Setup(new TActorSystemSetup())
        , LogSettings(new NActors::NLog::TSettings(NActors::TActorId(1, "logger"),
                                                   NActorsServices::LOGGER,
                                                   NActors::NLog::PRI_ERROR,
                                                   NActors::NLog::PRI_ERROR,
                                                   0))
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

        const ui32 numDevices = Cfg.NumDevices();
        PDiskActorIds.resize(numDevices);
        PDiskGuids.resize(numDevices);
        for (ui32 i = 0; i < numDevices; ++i) {
            PDiskActorIds[i] = MakeBlobStoragePDiskID(1, i + 1);
            PDiskGuids[i] = 12345 + i;
        }
        PDiskId = PDiskActorIds[0];
        PDiskGuid = PDiskGuids[0];
    }

    void FormatPDiskForTest() {
        for (ui32 i = 0; i < Cfg.NumDevices(); ++i) {
            FormatPDiskForDevice(i);
        }
    }

    void FormatPDiskForDevice(ui32 deviceIdx) {
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
        if (Cfg.SectorMaps[deviceIdx]) {
            options.SectorMap = Cfg.SectorMaps[deviceIdx];
            options.EnableSmallDiskOptimization = false;
            diskSize = Cfg.SectorMaps[deviceIdx]->DeviceSize;
        }
        FormatPDisk(Cfg.Paths[deviceIdx], diskSize, 4 << 10, ChunkSize, PDiskGuids[deviceIdx],
            chunkKey, logKey, sysLogKey, NPDisk::YdbDefaultPDiskSequence, "Info", options);
    }

    TIntrusivePtr<TPDiskConfig> MakePDiskConfig(ui32 deviceIdx) {
        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(
            Cfg.Paths[deviceIdx], PDiskGuids[deviceIdx], deviceIdx + 1,
            TPDiskCategory(Cfg.DeviceType, 0).GetRaw());
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
        if (Cfg.SectorMaps[deviceIdx]) {
            pDiskConfig->SectorMap = Cfg.SectorMaps[deviceIdx];
        }
        if (!TestProto.GetEnableTrim()) {
            pDiskConfig->DriveModelTrimSpeedBps = 0;
        }
        return pDiskConfig;
    }

    void DoBasicSetup() {
        Counters = TIntrusivePtr<NMonitoring::TDynamicCounters>(new NMonitoring::TDynamicCounters());

        TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());

        Setup->NodeId = 1;
        Setup->ExecutorsCount = 4;
        Setup->Executors.Reset(new TAutoPtr<IExecutorPool>[4]);
        Setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20, "nameservice"));
        Setup->Executors[1].Reset(new TBasicExecutorPool(1, std::max(1u, Cfg.NumDevices()), 20, "pdisk_actors"));
        Setup->Executors[2].Reset(new TBasicExecutorPool(2, std::max(4u, Cfg.NumDevices() + 1), 20, "perf_actors"));
        Setup->Executors[3].Reset(new TIOExecutorPool(3, 1, "IO"));
        Setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(64, 20)));

        const TActorId nameserviceId = GetNameserviceActorId();
        TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
        Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(nameserviceId, std::move(nameserviceSetup)));

        FormatPDiskForTest();

        for (ui32 i = 0; i < Cfg.NumDevices(); ++i) {
            auto pDiskConfig = MakePDiskConfig(i);

            if (i == 0) {
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
                if (Cfg.NumDevices() > 1) {
                    Printer->AddGlobalParam("NumDevices", Cfg.NumDevices());
                }
            }

            TActorSetupCmd pDiskSetup(CreatePDisk(pDiskConfig.Get(),
                        NPDisk::TMainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence }, .IsInitialized = true }, Counters),
                        TMailboxType::ReadAsFilled, 1);
            Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(PDiskActorIds[i], std::move(pDiskSetup)));
        }

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
        LogSettings->SetLevel(NLog::PRI_ERROR, NKikimrServices::BS_DDISK, explanation);

        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(LogSettings, NActors::CreateStderrBackend(),
            GetServiceCounters(Counters, "utils"));
        NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 3);
        std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(NActors::TActorId(1, "logger"),
            std::move(loggerActorCmd));
        Setup->LocalServices.push_back(std::move(loggerActorPair));
    }

    TVector<TPDiskDeviceInfo> GetDeviceInfos() const {
        TVector<TPDiskDeviceInfo> devices;
        devices.reserve(PDiskActorIds.size());
        for (ui32 i = 0; i < PDiskActorIds.size(); ++i) {
            devices.push_back({i + 1, PDiskGuids[i]});
        }
        return devices;
    }

    virtual void SetupLoadActor() {
        TestId = MakeBlobStorageProxyID(1);
        TActorSetupCmd testSetup(new TPerfTestActor(GetDeviceInfos(), Cfg, TestProto, Printer, Counters),
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

    ui32 EventsPerTest() const {
        return Cfg.NumDevices() == 1 ? 1 : Cfg.NumDevices() + 1;
    }

    void Run() override {
        if (IsLastExceptionSet) {
            return;
        }

        try {
            ActorSystem->Send(TestId, new TEvTablet::TEvBoot(MakeTabletID(0, 0, 1), 0, nullptr, TActorId(), nullptr));

            const ui32 eventsPerTest = EventsPerTest();
            for (ui32 i = 0; i < TestProto.PDiskTestListSize(); ++i) {
                for (ui32 j = 0; j < eventsPerTest; ++j) {
                    DoneEvent.Wait();
                    Printer->PrintResults();
                    ResultsPrintedEvent.Signal();
                }
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
