#include "prepare.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/vdisk_services.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>

#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/scheme/scheme_type_registry.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/dirut.h>

using namespace NKikimr;

//////////////////////////////////////////////////////////////////////////////////////
// TAllPDisksConfiguration
//////////////////////////////////////////////////////////////////////////////////////
TAllPDisksConfiguration TAllPDisksConfiguration::MkDevice(const TString &devicePath,
                                                          ui32 chunkSize,
                                                          TString deviceType) {
    return TAllPDisksConfiguration(1, chunkSize, 0, TString(), devicePath, deviceType);
}

TAllPDisksConfiguration TAllPDisksConfiguration::MkOneTmp(ui32 chunkSize,
                                                          ui64 diskSize,
                                                          TString deviceType) {
    return TAllPDisksConfiguration(1, chunkSize, diskSize, TString(), TString(), deviceType);
}

TAllPDisksConfiguration TAllPDisksConfiguration::MkManyTmp(ui32 pDisksNum,
                                                           ui32 chunkSize,
                                                           ui64 diskSize,
                                                           TString deviceType) {
    Y_ASSERT(pDisksNum > 0);
    return TAllPDisksConfiguration(pDisksNum, chunkSize, diskSize, TString(), TString(), deviceType);
}

TAllPDisksConfiguration::TAllPDisksConfiguration(ui32 num, ui32 chunkSize, ui64 diskSize,
                                                 const TString &dir, const TString &device,
                                                 TString deviceType)
    : PDisksNum(num)
    , ChunkSize(chunkSize)
    , DiskSize(diskSize)
    , Dir(dir)
    , Device(device)
    , DeviceType(deviceType)
{}


//////////////////////////////////////////////////////////////////////////////////////
// TOnePDisk
//////////////////////////////////////////////////////////////////////////////////////
TOnePDisk::TOnePDisk(ui32 pDiskId, ui64 pDiskGuid, const TString &filename,
                     ui32 chunkSize, ui64 diskSize)
    : PDiskActorID()
    , PDiskID(pDiskId)
    , PDiskGuid(pDiskGuid)
    , Filename(filename)
    , ChunkSize(chunkSize)
    , DiskSize(diskSize)
{}

void TOnePDisk::FormatDisk(bool force) {
    if (force || !NFs::Exists(Filename)) {
        const NPDisk::TKey chunkKey = RandomNumber<ui64>();
        const NPDisk::TKey logKey = RandomNumber<ui64>();
        const NPDisk::TKey sysLogKey = RandomNumber<ui64>();
        FormatPDisk(Filename,       // path
                    DiskSize,       // diskSizeBytes                // 0 for device
                    4 << 10,        // sectorSizeBytes
                    ChunkSize,      // userAccessibleChunkSizeBytes
                    PDiskGuid,      // diskGuid
                    chunkKey,       // chunkKey
                    logKey,         // logKey
                    sysLogKey,      // sysLogKey
                    NPDisk::YdbDefaultPDiskSequence,          // mainKey
                    "",             // textMessage
                    false,          // isErasureEncode
                    false,          // trimEntireDevice
                    nullptr,        // sectorMap
                    false           // enableSmallDiskOptimization
                    );
    }
}

void TOnePDisk::EraseDisk(ui64 newGuid) {
    PDiskGuid = newGuid;
    FormatDisk(true);
}


//////////////////////////////////////////////////////////////////////////////////////
// TAllPDisks
//////////////////////////////////////////////////////////////////////////////////////
TAllPDisks::TAllPDisks(const TAllPDisksConfiguration &cfg)
    : Cfg(cfg)
    , PDisks()
    , TempDir()
{
    if (cfg.Device.empty()) {
        // using filesystem
        TString entryDir;
        if (cfg.Dir.empty()) {
            TempDir = std::make_shared<TTempDir>();
            entryDir = (*TempDir)();
        } else {
            entryDir = cfg.Dir;
        }

        // database dir
        TString dbDir = Sprintf("%s/yard", entryDir.data());
        MakeDirIfNotExist(dbDir.c_str());

        // create pdisks
        PDisks.reserve(cfg.PDisksNum);
        for (ui32 i = 0; i < cfg.PDisksNum; i++) {
            // pdisk specific parameters
            ui32 pDiskId = i + 1;
            ui64 pDiskGuid = i + 1; // some guide != 0
            TString filename = Sprintf("%s/pdisk%u.dat", dbDir.data(), pDiskId);

            // create pdisk
            PDisks.emplace_back(pDiskId, pDiskGuid, filename, cfg.ChunkSize, cfg.DiskSize);

            // format pdisk
            TOnePDisk &inst = PDisks[i];
            inst.FormatDisk(false);
        }
    } else {
        // using device
        Y_ABORT_UNLESS(cfg.PDisksNum == 1 && cfg.DiskSize == 0);
        ui32 pDiskId = 1;
        ui64 pDiskGuid = 1; // some guide != 0

        // create pdisk
        PDisks.emplace_back(pDiskId, pDiskGuid, cfg.Device, cfg.ChunkSize, cfg.DiskSize);

        // format pdisk
        TOnePDisk &inst = PDisks[0];
        inst.FormatDisk(true);
    }
}

void TAllPDisks::ActorSetupCmd(NActors::TActorSystemSetup *setup, ui32 node,
                               const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
    for (ui32 i = 0; i < PDisks.size(); i++) {
        TOnePDisk &inst = PDisks[i];
        inst.PDiskActorID = MakeBlobStoragePDiskID(node, i);
        TIntrusivePtr<TPDiskConfig> pDiskConfig;
        NPDisk::EDeviceType deviceType = NPDisk::DeviceTypeFromStr(Cfg.DeviceType);
        pDiskConfig.Reset(new TPDiskConfig(inst.Filename, inst.PDiskGuid, inst.PDiskID,
                                           TPDiskCategory(deviceType, 0).GetRaw()));
        pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        const NPDisk::TMainKey mainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence }, .IsInitialized = true };
        TActorSetupCmd pDiskSetup(CreatePDisk(pDiskConfig.Get(),
                    mainKey, counters), TMailboxType::Revolving, 0);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(inst.PDiskActorID, std::move(pDiskSetup)));
    }
}

TOnePDisk &TAllPDisks::Get(ui32 pDiskID) {
    Y_ASSERT(pDiskID > 0);
    return PDisks.at(pDiskID - 1);
}

void TAllPDisks::EraseDisk(ui32 diskNum, ui64 newGuid) {
    TOnePDisk &inst = PDisks.at(diskNum);
    inst.PDiskGuid = newGuid;
    inst.FormatDisk(true);
}




//////////////////////////////////////////////////////////////////////////////////////
// TAllVDisks
//////////////////////////////////////////////////////////////////////////////////////
TAllVDisks::TAllVDisks(TAllPDisks *pdisks, ui32 domainsNum, ui32 disksInDomain, IVDiskSetup *vdiskSetup, bool onePDisk,
        bool runRepl, ui64 *inOutInitOwnerRound)
    : DomainsNum(domainsNum)
    , DisksInDomain(disksInDomain)
{
    VDisks.resize(DomainsNum * DisksInDomain);

    // create vdisks
    for (ui32 d = 0; d < DomainsNum; d++) {
        for (ui32 j = 0; j < DisksInDomain; j++) {
            ui32 id = d * DisksInDomain + j;
            TAllVDisks::TVDiskInstance &vdisk = VDisks.at(id);
            ui32 pDiskID = onePDisk ? 1 : id + 1;
            ui32 vdiskSlotId = onePDisk ? id : 0;
            vdisk.Initialized = vdiskSetup->SetUp(vdisk, pdisks, id, d, j, pDiskID, vdiskSlotId, runRepl,
                    *inOutInitOwnerRound);
            (*inOutInitOwnerRound)++;
        }
    }
}

TAllVDisks::~TAllVDisks() {
}

void TAllVDisks::ActorSetupCmd(NActors::TActorSystemSetup *setup, NKikimr::TBlobStorageGroupInfo *groupInfo,
                               const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
    for (ui32 i = 0; i < VDisks.size(); i++) {
        TVDiskInstance &vdisk = VDisks[i];
        if (vdisk.Initialized) {
            TActorSetupCmd vdiskSetup(CreateVDisk(vdisk.Cfg.Get(), groupInfo, counters), TMailboxType::Revolving, 0);
            setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(vdisk.ActorID, std::move(vdiskSetup)));
        }
    }
}

TDefaultVDiskSetup::TDefaultVDiskSetup() {
    auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
        cfg->LevelCompaction = true;
        cfg->FreshCompaction = true;
        cfg->GCOnlySynced = true;
        cfg->RecoveryLogCutterFirstDuration = TDuration::Seconds(3);
        cfg->RecoveryLogCutterRegularDuration = TDuration::Seconds(5);
        cfg->AdvanceEntryPointTimeout = TDuration::Seconds(5);
        cfg->MaxLogoBlobDataSize = 128u << 10u;
        cfg->MinHugeBlobInBytes = 64u << 10u;
        cfg->MilestoneHugeBlobInBytes = 64u << 10u;
        cfg->SyncTimeInterval = TDuration::Seconds(1);
        cfg->SyncJobTimeout = TDuration::Seconds(20);
        cfg->RunSyncer = true;
        cfg->ReplTimeInterval = TDuration::Seconds(10);
        cfg->SkeletonFrontQueueBackpressureCheckMsgId = false;
    };
    AddConfigModifier(modifier);
}

bool TDefaultVDiskSetup::SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j,
                               ui32 pDiskID, ui32 slotId, bool runRepl, ui64 initOwnerRound) {
    TOnePDisk &pdisk = pdisks->Get(pDiskID);
    vdisk.ActorID = MakeBlobStorageVDiskID(1, id + 1, 0);
    vdisk.VDiskID = TVDiskID(TGroupId::Zero(), 1, 0, d, j);

    NKikimr::TVDiskConfig::TBaseInfo baseInfo(vdisk.VDiskID, pdisk.PDiskActorID, pdisk.PDiskGuid,
            pdisk.PDiskID, NKikimr::NPDisk::DEVICE_TYPE_ROT, slotId,
            NKikimrBlobStorage::TVDiskKind::Default, initOwnerRound, {});
    vdisk.Cfg = MakeIntrusive<NKikimr::TVDiskConfig>(baseInfo);

    for (auto &modifier : ConfigModifiers) {
        modifier(vdisk.Cfg.Get());
    }
    vdisk.Cfg->RunRepl = runRepl;
    vdisk.Cfg->UseCostTracker = false;

    return true;
}


//////////////////////////////////////////////////////////////////////////////////////
// TConfiguration
//////////////////////////////////////////////////////////////////////////////////////
TConfiguration::TConfiguration(const TAllPDisksConfiguration &pcfg,
                               ui32 domainsNum,
                               ui32 disksInDomain,
                               TErasureType::EErasureSpecies erasure)
    : DomainsNum(domainsNum)
    , DisksInDomain(disksInDomain)
    , Erasure(erasure)
    , PCfg(pcfg)
{}

TConfiguration::~TConfiguration() {
    if (ActorSystem1) {
        Shutdown();
    }
}

static TProgramShouldContinue KikimrShouldContinue;

void TConfiguration::Prepare(IVDiskSetup *vdiskSetup, bool newPDisks, bool runRepl) { // FIXME: put newPDisks into configuration (see up)
    Counters = TIntrusivePtr<::NMonitoring::TDynamicCounters>(new ::NMonitoring::TDynamicCounters());

    TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
    TPortManager pm;
    nameserverTable->StaticNodeTable[1] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12001));
    nameserverTable->StaticNodeTable[2] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12002));

    THolder<TActorSystemSetup> setup1(new TActorSystemSetup());
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

    ui64 initOwnerRound = 1;
    // setup pdisks
    if (newPDisks) {
        PDisks.reset(new TAllPDisks(PCfg));
    }
    PDisks->ActorSetupCmd(setup1.Get(), setup1->NodeId, Counters);

    // setup GroupInfo
    GroupInfo = new TBlobStorageGroupInfo(Erasure, DisksInDomain, DomainsNum);

    // create vdisks
    initOwnerRound += 100;
    VDisks.reset(new TAllVDisks(PDisks.get(), DomainsNum, DisksInDomain, vdiskSetup,
                                (PCfg.PDisksNum == 1), runRepl, &initOwnerRound));
    VDisks->ActorSetupCmd(setup1.Get(), GroupInfo.Get(), Counters);

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
    //logSettings->SetLevel(NLog::PRI_INFO, NKikimrServices::BS_HULLCOMP, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_HULLQUERY, explanation);
    //logSettings->SetLevel(NLog::PRI_ERROR, NKikimrServices::BS_SYNCLOG, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_SYNCJOB, explanation);
    //logSettings->SetLevel(NLog::PRI_NOTICE, NKikimrServices::BS_SYNCER, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_REPL, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_HANDOFF, explanation);
    //logSettings->SetLevel(NLog::PRI_INFO, NKikimrServices::BS_PDISK, explanation);

    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_BLOCK, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_GC, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_GET, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_PUT, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_OTHER, explanation);

    //logSettings->SetLevel(NLog::PRI_INFO, NActorsServices::TEST, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_HULLHUGE, explanation);
    //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_PDISK, explanation);

    NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(logSettings,
                                                                   NActors::CreateStderrBackend(),
                                                                   Counters->GetSubgroup("logger", "counters"));
    NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 0);
    std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(loggerActorId, std::move(loggerActorCmd));
    setup1->LocalServices.push_back(std::move(loggerActorPair));
    //////////////////////////////////////////////////////////////////////////////

    ///////////////////////// MONITORING SETTINGS /////////////////////////////////
    Monitoring.reset(new NActors::TSyncHttpMon({
        .Port = 8088,
        .Title = "at"
    }));
    NMonitoring::TIndexMonPage *actorsMonPage = Monitoring->RegisterIndexPage("actors", "Actors");
    Monitoring->RegisterCountersPage("counters", "Counters", Counters);
    Monitoring->Start();
    loggerActor->Log(Now(), NKikimr::NLog::PRI_NOTICE, NActorsServices::TEST, "Monitoring settings set up");
    //////////////////////////////////////////////////////////////////////////////

    TIntrusivePtr<NScheme::TTypeRegistry> typeRegistry(new NScheme::TKikimrTypeRegistry());
    AppData.reset(new NKikimr::TAppData(0, 1, 2, 3, TMap<TString, ui32>(), typeRegistry.Get(),
                                        nullptr, nullptr, &KikimrShouldContinue));
    AppData->Counters = Counters;
    AppData->Mon = Monitoring.get();
    IoContext = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
    AppData->IoContextFactory = IoContext.get();

    ActorSystem1.reset(new TActorSystem(setup1, AppData.get(), logSettings));
    Monitoring->RegisterActorPage(actorsMonPage, "logger", "Logger", false, ActorSystem1.get(), loggerActorId);
    loggerActor->Log(Now(), NKikimr::NLog::PRI_NOTICE, NActorsServices::TEST, "Actor system created");

    ActorSystem1->Start();
    LOG_NOTICE(*ActorSystem1, NActorsServices::TEST, "Actor system started");

}

void TConfiguration::Shutdown() {
    ActorSystem1->Stop();
    ActorSystem1.reset();
}



class TDbInitWaitActor : public TActorBootstrapped<TDbInitWaitActor> {
    TConfiguration *Conf;
    unsigned Count;

    friend class TActorBootstrapped<TDbInitWaitActor>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);
        TLogoBlobID id(0, 1, 1, 0, 0, 0);
        ctx.Schedule(TDuration::Seconds(1000), new TEvents::TEvWakeup(), nullptr);
        for (ui32 i = 0; i < Conf->VDisks->GetSize(); ++i) {
            TAllVDisks::TVDiskInstance &vdisk = Conf->VDisks->Get(i);
            if (vdisk.Initialized) {
                auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdisk.VDiskID, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::NotifyIfNotReady, {}, {id});
                ctx.Send(vdisk.ActorID, req.release());
                ++Count;
            }
        }
    }

    void Finish(const TActorContext &ctx, bool ok) {
        LOG_NOTICE(ctx, NActorsServices::TEST, "%s", (ok ? "DB IS READY" : "DB INIT TIMEOUT"));
        Conf->DbInitEvent.Signal();
        Die(ctx);
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Record.GetStatus() != NKikimrProto::NOTREADY) {
            --Count;
        }

        if (Count == 0)
            Finish(ctx, true);
    }

    void Timeout(const TActorContext &ctx) {
        Finish(ctx, false);
    }

    void Handle(TEvBlobStorage::TEvVReadyNotify::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        --Count;
        if (Count == 0)
            Finish(ctx, true);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        HFunc(TEvBlobStorage::TEvVReadyNotify, Handle);
        CFunc(TEvents::TSystem::Wakeup, Timeout);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TDbInitWaitActor(TConfiguration *conf)
        : TActorBootstrapped<TDbInitWaitActor>()
        , Conf(conf)
        , Count(0)
    {}
};


void TConfiguration::DbInitWait() {
    ActorSystem1->Register(new TDbInitWaitActor(this));
    DbInitEvent.Wait();
}

void TConfiguration::PoisonVDisks() {
    for (ui32 i = 0, s = VDisks->GetSize(); i < s; i++) {
        TAllVDisks::TVDiskInstance &v = VDisks->Get(i);
        ActorSystem1->Send(v.ActorID, new NActors::TEvents::TEvPoisonPill());
    }
}

void TConfiguration::PoisonPDisks() {
    // FIXME: implement
    Y_DEBUG_ABORT_UNLESS(false);
}
