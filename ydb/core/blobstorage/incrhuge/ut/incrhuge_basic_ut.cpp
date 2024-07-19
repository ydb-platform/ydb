#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/core/blobstorage/incrhuge/incrhuge.h>
#include <ydb/core/blobstorage/incrhuge/incrhuge_keeper.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>
#include <util/folder/tempdir.h>
#include <util/folder/path.h>
#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NIncrHuge;

#include "test_actor_concurrent.h"
#include "test_actor_seq.h"
#include "faulty_pdisk.h"

class TTestEnv {
public:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters = new ::NMonitoring::TDynamicCounters;
    TString Path;
    ui32 ChunkSize;
    ui64 DiskSize;
    ui64 PDiskGuid;
    ui64 PDiskKey;
    NPDisk::TMainKey MainKey;
    TActorId PDiskId;
    TActorId KeeperId;
    std::unique_ptr<TActorSystem> ActorSystem;
    TTempDir TempDir;
    std::unique_ptr<TAppData> AppData;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;

    void Setup(bool format = true, ui32 counter = 0, TManualEvent *event = nullptr, ui32 numChunks = 1000,
            ui32 chunkSize = 16 << 20) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 3;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[3]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDisk
        if (format) {
            TString dir = TempDir();
            Path = dir + "/incrhuge.bin";
            TFsPath(Path).DeleteIfExists();
            ChunkSize = chunkSize;
            DiskSize = (ui64)ChunkSize * numChunks;
            PDiskGuid = Now().GetValue();
            PDiskKey = 1;
            MainKey = NPDisk::TMainKey{ .Keys = { 1 }, .IsInitialized = true };
            FormatPDisk(Path, DiskSize, 4096, ChunkSize, PDiskGuid, PDiskKey, PDiskKey, PDiskKey, MainKey.Keys.back(), "incrhuge",
                false, false, nullptr, false);
        }

        PDiskId = MakeBlobStoragePDiskID(1, 1);
        ui64 pDiskCategory = 0;
        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(Path, PDiskGuid, 1, pDiskCategory);
        TActorSetupCmd pDiskSetup(CreatePDisk(pDiskConfig.Get(), MainKey, Counters), TMailboxType::Revolving, 0);
        setup->LocalServices.emplace_back(PDiskId, std::move(pDiskSetup));

        TActorId pdiskActorId;
        if (counter) {
            char x[12] = {'f', 'a', 'u', 'l', 't', 'y', 'p', 'd', 'i', 's', 'k'};
            pdiskActorId = TActorId(0, x);
            setup->LocalServices.emplace_back(pdiskActorId, TActorSetupCmd(new TFaultyPDiskActor(PDiskId, counter, event),
                    TMailboxType::Simple, 0));
        } else {
            pdiskActorId = PDiskId;
        }

        TKeeperSettings settings;
        settings.PDiskId = 1;
        settings.PDiskActorId = pdiskActorId;
        settings.PDiskGuid = PDiskGuid;
        settings.MinCleanChunks = 8;
        settings.MinAllocationBatch = 4;
        settings.UnalignedBlockSize = 4096;
        settings.MinHugeBlobInBytes = 512 << 10;
        settings.MaxInFlightWrites = 5;
        settings.InitOwnerRound = 2;

        KeeperId = MakeIncrHugeKeeperId(1);
        TActorSetupCmd keeperSetup(CreateIncrHugeKeeper(settings), TMailboxType::Revolving, 0);
        setup->LocalServices.emplace_back(KeeperId, std::move(keeperSetup));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // LOGGER
        NActors::TActorId loggerActorId{1, "logger"};
        TIntrusivePtr<NActors::NLog::TSettings> logSettings{new NActors::NLog::TSettings{loggerActorId,
            NActorsServices::LOGGER, NActors::NLog::PRI_ERROR, NActors::NLog::PRI_ERROR, 0}};
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
        logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_INCRHUGE, explanation);
        logSettings->SetLevel(NLog::PRI_DEBUG, NActorsServices::TEST, explanation);

        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor{logSettings, NActors::CreateStderrBackend(),
            Counters};
        NActors::TActorSetupCmd loggerActorCmd{loggerActor, NActors::TMailboxType::Simple, 2};
        setup->LocalServices.emplace_back(loggerActorId, std::move(loggerActorCmd));
        AppData.reset(new TAppData(0, 1, 2, 1, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr));
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        AppData->IoContextFactory = IoContext.get();

        ActorSystem.reset(new TActorSystem{setup, AppData.get(), logSettings});
    }

    void Start() {
        ActorSystem->Start();
    }

    void Stop() {
        ActorSystem->Stop();
        ActorSystem.reset();
    }
};

Y_UNIT_TEST_SUITE(TIncrHugeBasicTest) {
    Y_UNIT_TEST(WriteReadDeleteEnum) {
        return; /* TODO(alexvru): KIKIMR-7588 */

        TTestEnv env;
        env.Setup();
        env.Start();
        TManualEvent event;
        TTestActorConcurrent::TTestActorState state;
        const ui32 numActions = NSan::PlainOrUnderSanitizer(
            NValgrind::PlainOrUnderValgrind(10000, 100),
            1000
        );
        TTestActorConcurrent *actor = new TTestActorConcurrent(env.KeeperId, &event, state, numActions, 1);
        env.ActorSystem->Register(actor);
        event.WaitI();
        env.Stop();
    }

    Y_UNIT_TEST(WriteReadDeleteEnumRecover) {
        return; /* TODO(alexvru): KIKIMR-7588 */

        TTestActorConcurrent::TTestActorState state;
        TManualEvent event;
        TTestActorConcurrent *actor;
        TTestEnv env;

        env.Setup();
        env.Start();
        actor = new TTestActorConcurrent(env.KeeperId, &event, state, 500, 1);
        env.ActorSystem->Register(actor);
        event.WaitI();
        usleep(150000);
        env.Stop();

        for (ui32 i = 0; i < 10; ++i) {
            event.Reset();

            env.Setup(false);
            env.Start();
            const ui32 numActions = NSan::PlainOrUnderSanitizer(100, 10);
            actor = new TTestActorConcurrent(env.KeeperId, &event, state, numActions, 2);
            env.ActorSystem->Register(actor);
            event.WaitI();
            env.Stop();
        }
    }

    Y_UNIT_TEST(Defrag) {
        TTestEnv env;
        env.Setup(true, 0, nullptr, 1000);
        env.Start();
        TManualEvent event;
        TTestActorConcurrent::TTestActorState state;
        const ui32 initialNumActions = NSan::PlainOrUnderSanitizer(1000, 100);
        TTestActorConcurrent *actor = new TTestActorConcurrent(env.KeeperId, &event, state, initialNumActions, 1,
            100000 /* writeScore */, 80000 /* deleteScore */, 0 /* readScore */);
        env.ActorSystem->Register(actor);
//        usleep(1 * 1000 * 1000);
//        env.ActorSystem->Send(new IEventHandle(env.KeeperId, id, new TEvIncrHugeControlDefrag(0.8)));
        event.WaitI();
        usleep(50 * 1000 * 1000); // wait for defragment
        env.Stop();

        event.Reset();

        env.Setup(false);
        env.Start();
        LOG_DEBUG(*env.ActorSystem, NActorsServices::TEST, "starting recovery");
        const ui32 numActions = NSan::PlainOrUnderSanitizer(5000, 1000);
        actor = new TTestActorConcurrent(env.KeeperId, &event, state, numActions, 2);
        env.ActorSystem->Register(actor);
        event.WaitI();
        env.Stop();
    }

    Y_UNIT_TEST(Recovery) {
        return; // TODO KIKIMR-3065
        TTestEnv env;
        env.Setup(true, 0, nullptr, 2000, 128 << 20);
        env.Start();
        TManualEvent event;
        TTestActorConcurrent::TTestActorState state;
        const ui32 numActions = NSan::PlainOrUnderSanitizer(33333, 1000);
        TTestActorConcurrent *actor = new TTestActorConcurrent(env.KeeperId, &event, state, numActions, 1,
            100000 /* writeScore */, 1000 /* deleteScore */, 0 /* readScore */);
        env.ActorSystem->Register(actor);
        event.WaitI();
        env.Stop();

        event.Reset();

        env.Setup(false);
        env.Start();
        LOG_DEBUG(*env.ActorSystem, NActorsServices::TEST, "starting recovery");
        actor = new TTestActorConcurrent(env.KeeperId, &event, state, 0, 2);
        env.ActorSystem->Register(actor);
        event.WaitI();
        env.Stop();
    }

/*    Y_UNIT_TEST(FaultyPDisk) {
        for (ui32 counter = 20; counter < 1000; ++counter) {
            TTestActorSeq::TTestActorState state;
            TTestActorSeq *actor;
            TTestEnv env;
            TManualEvent event;

            env.Setup(true, counter, &event);
            env.Start();
            LOG_DEBUG(*env.ActorSystem, NActorsServices::TEST, "=== starting Counter# %" PRIu32, counter);
            actor = new TTestActorSeq(env.KeeperId, state, 1, nullptr);
            env.ActorSystem->Register(actor);
            event.WaitI();
            env.Stop();

            event.Reset();

            env.Setup(false);
            env.Start();
            LOG_DEBUG(*env.ActorSystem, NActorsServices::TEST, "=== restarting 1");
            actor = new TTestActorSeq(env.KeeperId, state, 1, &event);
            env.ActorSystem->Register(actor);
            usleep(5 * 1000 * 1000);
            env.Stop();

            event.Reset();

            env.Setup(false);
            env.Start();
            LOG_DEBUG(*env.ActorSystem, NActorsServices::TEST, "=== restarting 2");
            actor = new TTestActorSeq(env.KeeperId, state, 1, &event);
            env.ActorSystem->Register(actor);
            event.WaitI();
            env.Stop();
        }
    }
*/
}
