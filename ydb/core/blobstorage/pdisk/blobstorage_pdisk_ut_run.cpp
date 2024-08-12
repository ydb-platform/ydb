#include "blobstorage_pdisk_ut_run.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_base_test.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/library/pdisk_io/aio.h>

#include <util/folder/tempdir.h>

namespace NKikimr {

namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}

void Run(TVector<IActor*> tests, TTestRunConfig runCfg) {
    TTempDir tempDir;
    TVector<TActorId> testIds;
    TActorId pDiskId;
    TAppData appData(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
    auto ioContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
    appData.IoContextFactory = ioContext.get();

    THolder<TActorSystem> actorSystem1;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> mainCounters;
    THolder<NActors::TMon> monitoring;

    TAtomic doneCounter = 0;
    TSystemEvent doneEvent(TSystemEvent::rAuto);
    yexception lastException;
    TAtomic isLastExceptionSet = 0;
    for (size_t i = 0; i < tests.size(); ++i) {
        auto *p = static_cast<TCommonBaseTest*>(tests[i]);
        p->Init(&doneCounter, &doneEvent, &lastException, &isLastExceptionSet, &runCfg.TestContext->PDiskGuid);
    }

    try {
        mainCounters = TIntrusivePtr<::NMonitoring::TDynamicCounters>(new ::NMonitoring::TDynamicCounters());

        testIds.resize(runCfg.Instances);

        TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
        TPortManager pm;
        nameserverTable->StaticNodeTable[1] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12001));
        nameserverTable->StaticNodeTable[2] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12002));

        THolder<TActorSystemSetup> setup1(new TActorSystemSetup());
        setup1->NodeId = 1;
        setup1->ExecutorsCount = 3;
        setup1->Executors.Reset(new TAutoPtr<IExecutorPool>[3]);
        setup1->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20));
        setup1->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup1->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup1->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

        const TActorId nameserviceId = GetNameserviceActorId();
        TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
        setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(nameserviceId, std::move(nameserviceSetup)));

        TString dataPath;
        if (!runCfg.TestContext->IsFormatedDiskExpected()) {
            if (runCfg.TestContext->Dir) {
                TString databaseDirectory = MakeDatabasePath(runCfg.TestContext->Dir);
                dataPath = MakePDiskPath(runCfg.TestContext->Dir);
                MakeDirIfNotExist(databaseDirectory.c_str());
            }

            EntropyPool().Read(&runCfg.TestContext->PDiskGuid, sizeof(runCfg.TestContext->PDiskGuid));
            if (!runCfg.IsBad) {
                FormatPDiskForTest(dataPath, runCfg.TestContext->PDiskGuid, runCfg.ChunkSize,
                        runCfg.IsErasureEncodeUserLog, runCfg.TestContext->SectorMap);
            }
        } else {
            Y_ABORT_UNLESS(!runCfg.IsBad);
        }

        pDiskId = MakeBlobStoragePDiskID(1, 1);
        ui64 pDiskCategory = 0;
        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(dataPath, runCfg.TestContext->PDiskGuid, 1, pDiskCategory);
        pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->ChunkSize = runCfg.ChunkSize;
        pDiskConfig->SectorMap = runCfg.TestContext->SectorMap;
        pDiskConfig->EnableSectorEncryption = !pDiskConfig->SectorMap;
        pDiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(false);

        NPDisk::TMainKey mainKey{ .Keys = {NPDisk::YdbDefaultPDiskSequence}, .IsInitialized = true };
        TActorSetupCmd pDiskSetup(CreatePDisk(pDiskConfig.Get(),
                    mainKey, mainCounters), TMailboxType::Revolving, 0);
        setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(pDiskId, std::move(pDiskSetup)));

        for (ui32 i = 0; i < runCfg.Instances; ++i) {
            testIds[i] = MakeBlobStorageProxyID(1 + i);
            TActorSetupCmd testSetup(tests[i], TMailboxType::Revolving, 0);
            setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(testIds[i], std::move(testSetup)));
        }

        AtomicSet(doneCounter, 0);


        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters());
        /////////////////////// LOGGER ///////////////////////////////////////////////
        GetServiceCounters(counters, "utils");

        NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
        TIntrusivePtr<NActors::NLog::TSettings> logSettings(
            new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER,
                NActors::NLog::PRI_ERROR, NActors::NLog::PRI_ERROR, 0));
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
        if (!IsLowVerbose) {
            // We use Null log backend and test log message generation in non-verbose tests
            logSettings->SetLevel(NLog::PRI_TRACE, NKikimrServices::BS_PDISK, explanation);
        } else {
            logSettings->SetLevel(NLog::PRI_NOTICE, NKikimrServices::BS_PDISK, explanation);
        }

        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(logSettings,
            IsLowVerbose ?  NActors::CreateStderrBackend() : NActors::CreateNullBackend(),
            GetServiceCounters(counters, "utils"));
        NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 2);
        std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(loggerActorId, std::move(loggerActorCmd));
        setup1->LocalServices.push_back(std::move(loggerActorPair));
        //////////////////////////////////////////////////////////////////////////////

        actorSystem1.Reset(new TActorSystem(setup1, &appData, logSettings));

        if (IsMonitoringEnabled) {
            // Monitoring startup
            monitoring.Reset(new NActors::TSyncHttpMon({
                .Port = pm.GetPort(8081),
                .Title = "TestYard monitoring"
            }));
            appData.Mon = monitoring.Get();

            monitoring->RegisterCountersPage("counters", "Counters", mainCounters);
            monitoring->Start();
        }

        actorSystem1->Start();
        Sleep(TDuration::MilliSeconds(runCfg.BeforeTestSleepMs));

        VERBOSE_COUT("Sending TEvBoot to test");
        for (ui32 i = 0; i < runCfg.Instances; ++i) {
            actorSystem1->Send(testIds[i], new TEvTablet::TEvBoot(
                        MakeTabletID(false, 1), 0, nullptr, TActorId(), nullptr));
        }

        TAtomicBase doneCount = 0;
        bool isOk = true;
        TInstant startTime = Now();
        while (doneCount < runCfg.Instances && isOk) {
            ui32 msRemaining = TEST_TIMEOUT - (ui32)(Now() - startTime).MilliSeconds();
            isOk = doneEvent.Wait(msRemaining);
            doneCount = AtomicGet(doneCounter);
        }

        TIntrusivePtr<::NMonitoring::TDynamicCounters> pDiskCounters =
                GetServiceCounters(mainCounters, "pdisks")->GetSubgroup(
                        "pdisk", Sprintf("%09" PRIu32, (ui32)pDiskConfig->PDiskId));
        TIntrusivePtr<::NMonitoring::TDynamicCounters> deviceGroup = pDiskCounters->GetSubgroup("subsystem", "device");

        TStringStream errorStr;
        errorStr << "test timeout"
            << "; elapsed# " << deviceGroup->GetCounter("DeviceActualCostNs")->Val() / 1e9 << "sec"
            << "; bytesReadAndWritten# " << deviceGroup->GetCounter("DeviceBytesRead")->Val()
                + deviceGroup->GetCounter("DeviceBytesWritten")->Val()
            << "; IOs done# " << deviceGroup->GetCounter("DeviceReads")->Val()
                + deviceGroup->GetCounter("DeviceWrites")->Val();
        UNIT_ASSERT_VALUES_EQUAL_C(doneCount, runCfg.Instances, errorStr.Str());
    } catch (yexception ex) {
        lastException = ex;
        AtomicSet(isLastExceptionSet, 1);
    }

    monitoring.Destroy();
    if (actorSystem1.Get()) {
        actorSystem1->Stop();
        actorSystem1.Destroy();
    }
    doneEvent.Reset();
    if (AtomicGet(isLastExceptionSet)) {
        AtomicSet(isLastExceptionSet, 0);
        Cerr << lastException.what() << Endl;
        ythrow lastException;
    }
}

} // NKikimr
