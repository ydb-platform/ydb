#pragma once

#include "defs.h"

#include <ydb/core/protos/blob_depot_config.pb.h>

using namespace NActors;
using namespace NKikimr;

struct TEnvironmentSetup {
    std::unique_ptr<TTestActorSystem> Runtime;
    const TString DomainName = "Root";
    const ui32 DomainId = 1;
    const ui32 StaticGroupId = 0;
    const TString StoragePoolName = "test";
    TIntrusivePtr<NFake::TProxyDS> Group0 = MakeIntrusive<NFake::TProxyDS>();
    std::map<std::pair<ui32, ui32>, TIntrusivePtr<TPDiskMockState>> PDiskMockStates;
    NKikimr::NTestShard::TTestShardContext::TPtr TestShardContext = NKikimr::NTestShard::TTestShardContext::Create();
    NKikimr::NTesting::TGroupOverseer GroupOverseer;

    struct TSettings {
        const ui32 NodeCount = 8;
        const bool Encryption = false;
        const ui32 ControllerNodeId = 1;
    };

    const TSettings Settings;

    class TMockPDiskServiceFactory : public IPDiskServiceFactory {
        TEnvironmentSetup& Env;

    public:
        TMockPDiskServiceFactory(TEnvironmentSetup& env)
            : Env(env)
        {}

        void Create(const TActorContext& ctx, ui32 pdiskId, const TIntrusivePtr<TPDiskConfig>& cfg,
                const NPDisk::TMainKey& /*mainKey*/, ui32 poolId, ui32 nodeId) override {
            const auto key = std::make_pair(nodeId, pdiskId);
            TIntrusivePtr<TPDiskMockState>& state = Env.PDiskMockStates[key];
            if (!state) {
                state.Reset(new TPDiskMockState(nodeId, pdiskId, cfg->PDiskGuid, ui64(10) << 40, cfg->ChunkSize));
            }
            const TActorId& actorId = ctx.Register(CreatePDiskMockActor(state), TMailboxType::HTSwap, poolId);
            const TActorId& serviceId = MakeBlobStoragePDiskID(nodeId, pdiskId);
            ctx.ExecutorThread.ActorSystem->RegisterLocalService(serviceId, actorId);
        }
    };

    static TEnvironmentSetup *Env;

    TEnvironmentSetup(TSettings&& settings)
        : Settings(std::move(settings))
    {
        Y_ABORT_UNLESS(!Env);
        Env = this;

        struct TSetupEnv { TSetupEnv() { TEnvironmentSetup::SetupEnv(); } };
        Singleton<TSetupEnv>();
        Initialize();
    }

    ~TEnvironmentSetup() {
        Cleanup();
        Y_ABORT_UNLESS(Env == this);
        Env = nullptr;
    }

    static void SetupEnv() {
        TAppData::TimeProvider = TTestActorSystem::CreateTimeProvider();
        ui64 seed = RandomNumber<ui64>();
        if (const TString& s = GetEnv("SEED", "")) {
            seed = FromString<ui64>(s);
        }
        SetRandomSeed(seed);
        TAppData::RandomProvider = CreateDeterministicRandomProvider(RandomNumber<ui64>());
        Cerr << "RandomSeed# " << seed << Endl;
    }

    std::unique_ptr<TTestActorSystem> MakeRuntime() {
        auto domainsInfo = MakeIntrusive<TDomainsInfo>();
        auto domain = TDomainsInfo::TDomain::ConstructEmptyDomain(DomainName, DomainId);
        domainsInfo->AddDomain(domain.Get());
        domainsInfo->AddHive(MakeDefaultHiveID());
        return std::make_unique<TTestActorSystem>(Settings.NodeCount, NLog::PRI_ERROR, domainsInfo);
    }

    void Initialize() {
        Runtime = MakeRuntime();

        Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            GroupOverseer.ExamineEvent(nodeId, *ev);
            return true;
        };
        Runtime->FilterEnqueue = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev, ISchedulerCookie*, TInstant) {
            GroupOverseer.ExamineEnqueue(nodeId, *ev);
            return true;
        };

        SetupLogging();
        Runtime->Start();
        Runtime->SetupTabletRuntime(1, Settings.ControllerNodeId);
        SetupStaticStorage();
        for (ui32 nodeId : Runtime->GetNodes()) {
            SetupTablet(nodeId);
        }
        SetupStorage();
    }

    void StopNode(ui32 nodeId) {
        Runtime->StopNode(nodeId);
    }

    void StartNode(ui32 nodeId) {
        Runtime->StartNode(nodeId);
        Runtime->SetupTabletRuntime(1, Settings.ControllerNodeId, nodeId);
        SetupStaticStorage();
        SetupTablet(nodeId);
        SetupStorage(nodeId);
    }

    void RestartNode(ui32 nodeId) {
        StopNode(nodeId);
        StartNode(nodeId);
    }

    void Cleanup() {
        Runtime->Stop();
        Runtime.reset();
    }

    NKikimrBlobStorage::TConfigResponse Invoke(const NKikimrBlobStorage::TConfigRequest& request) {
        const TActorId edge = Runtime->AllocateEdgeActor(Settings.ControllerNodeId, __FILE__, __LINE__);

        const TActorId clientId = Runtime->Register(NKikimr::NTabletPipe::CreateClient(edge, MakeBSControllerID(),
            NTabletPipe::TClientRetryPolicy::WithRetries()), edge.NodeId());

        {
            auto response = Runtime->WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
        }

        Runtime->WrapInActorContext(edge, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
            ev->Record.MutableRequest()->CopyFrom(request);
            NTabletPipe::SendData(edge, clientId, ev.release());
        });

        auto res = Runtime->WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerConfigResponse>(edge)->Get()->Record.GetResponse();
        Runtime->Send(new IEventHandle(clientId, edge, new NKikimr::TEvTabletPipe::TEvShutdown()), edge.NodeId());
        return res;
    }

    void SetupLogging() {
//        Runtime->SetLogPriority(NKikimrServices::BS_HULLCOMP, NLog::PRI_NOTICE);
//        Runtime->SetLogPriority(NKikimrServices::BS_VDISK_SCRUB, NLog::PRI_NOTICE);
//
//        auto prio = NLog::PRI_ERROR;
//        Runtime->SetLogPriority(NKikimrServices::TABLET_MAIN, prio);
//        Runtime->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, prio);
//        Runtime->SetLogPriority(NKikimrServices::PIPE_CLIENT, prio);
//        Runtime->SetLogPriority(NKikimrServices::PIPE_SERVER, prio);
//        Runtime->SetLogPriority(NKikimrServices::TABLET_RESOLVER, prio);
//        Runtime->SetLogPriority(NKikimrServices::STATESTORAGE, prio);
//        Runtime->SetLogPriority(NKikimrServices::BOOTSTRAPPER, prio);
//        Runtime->SetLogPriority(NKikimrServices::BS_NODE, prio);

        std::initializer_list<ui32> debug{
//            NKikimrServices::BS_CONTROLLER,
//            NKikimrServices::BS_SELFHEAL,
//            NKikimrServices::BS_PDISK,
//            NKikimrServices::BS_SKELETON,
//            NKikimrServices::BS_HULLCOMP,
//            NKikimrServices::BS_HULLRECS,
//            NKikimrServices::BS_HULLHUGE,
//            NKikimrServices::BS_REPL,
//            NKikimrServices::BS_SYNCER,
//            NKikimrServices::BS_SYNCLOG,
//            NKikimrServices::BS_SYNCJOB,
//            NKikimrServices::BS_QUEUE,
//            NKikimrServices::BS_VDISK_GET,
//            NKikimrServices::BS_VDISK_PATCH,
//            NKikimrServices::BS_VDISK_PUT,
//            NKikimrServices::BS_VDISK_OTHER,
//            NKikimrServices::BS_PROXY,
//            NKikimrServices::BS_PROXY_RANGE,
//            NKikimrServices::BS_PROXY_GET,
//            NKikimrServices::BS_PROXY_PUT,
//            NKikimrServices::BS_PROXY_INDEXRESTOREGET,
//            NKikimrServices::BS_PROXY_STATUS,
            NActorsServices::TEST,
//            NKikimrServices::BLOB_DEPOT,
//            NKikimrServices::BLOB_DEPOT_AGENT,
//            NKikimrServices::HIVE,
//            NKikimrServices::LOCAL,
//            NKikimrServices::TEST_SHARD,
//            NActorsServices::INTERCONNECT,
//            NActorsServices::INTERCONNECT_SESSION,
        };
        for (const auto& comp : debug) {
            Runtime->SetLogPriority(comp, NLog::PRI_DEBUG);
        }

//        Runtime->SetLogPriority(NKikimrServices::TEST_SHARD, NLog::PRI_INFO);
    }

    void SetupStaticStorage() {
        const TActorId proxyId = MakeBlobStorageProxyID(StaticGroupId);
        for (const ui32 nodeId : Runtime->GetNodes()) {
            Runtime->RegisterService(proxyId, Runtime->Register(CreateBlobStorageGroupProxyMockActor(Group0), nodeId));
        }
    }

    void SetupStorage(ui32 targetNodeId = 0) {
        for (ui32 nodeId : Runtime->GetNodes()) {
            if (targetNodeId && nodeId != targetNodeId) {
                continue;
            }

            auto config = MakeIntrusive<TNodeWardenConfig>(new TMockPDiskServiceFactory(*this));
            config->BlobStorageConfig.MutableServiceSet()->AddAvailabilityDomains(DomainId);
            std::unique_ptr<IActor> warden(CreateBSNodeWarden(config));

            const TActorId wardenId = Runtime->Register(warden.release(), nodeId);
            Runtime->RegisterService(MakeBlobStorageNodeWardenID(nodeId), wardenId);
        }
    }

    void SetupTablet(ui32 nodeId) {
        auto localConfig = MakeIntrusive<TLocalConfig>();

        localConfig->TabletClassInfo[TTabletTypes::BlobDepot] = TLocalConfig::TTabletClassInfo(new TTabletSetupInfo(
            &NBlobDepot::CreateBlobDepot, TMailboxType::ReadAsFilled, Runtime->SYSTEM_POOL_ID, TMailboxType::ReadAsFilled,
            Runtime->SYSTEM_POOL_ID));

        localConfig->TabletClassInfo[TTabletTypes::TestShard] = TLocalConfig::TTabletClassInfo(new TTabletSetupInfo(
            &NKikimr::NTestShard::CreateTestShard, TMailboxType::ReadAsFilled, Runtime->SYSTEM_POOL_ID,
            TMailboxType::ReadAsFilled, Runtime->SYSTEM_POOL_ID));

        auto tenantPoolConfig = MakeIntrusive<TTenantPoolConfig>(localConfig);
        tenantPoolConfig->AddStaticSlot(DomainName);

        Runtime->RegisterService(MakeTenantPoolRootID(),
            Runtime->Register(CreateTenantPool(tenantPoolConfig), nodeId));
        Runtime->RegisterService(NSysView::MakeSysViewServiceID(nodeId),
            Runtime->Register(NSysView::CreateSysViewServiceForTests().Release(), nodeId));
        Runtime->Register(CreateTenantNodeEnumerationPublisher(), nodeId);
        Runtime->Register(CreateLabelsMaintainer({}), nodeId);

        const ui32 numChannels = 2;

        struct TTabletInfo {
            ui64 TabletId;
            TTabletTypes::EType Type;
            IActor* (*Create)(const TActorId&, TTabletStorageInfo*);
        };
        std::vector<TTabletInfo> tablets{
            {MakeBSControllerID(), TTabletTypes::BSController, &CreateFlatBsController},
        };

        if (const ui64 tabletId = Runtime->GetDomainsInfo()->GetHive(); tabletId != TDomainsInfo::BadTabletId) {
            tablets.push_back(TTabletInfo{tabletId, TTabletTypes::Hive, &CreateDefaultHive});
        }

        for (const TTabletInfo& tablet : tablets) {
            if (nodeId == Settings.ControllerNodeId) {
                Runtime->CreateTestBootstrapper(
                    TTestActorSystem::CreateTestTabletInfo(tablet.TabletId, tablet.Type, TBlobStorageGroupType::ErasureNone,
                    StaticGroupId, numChannels), tablet.Create, nodeId);
            }
        }

        Runtime->RegisterService(NKikimr::NTestShard::MakeStateServerInterfaceActorId(), Runtime->Register(
            CreateStateServerInterfaceActor(TestShardContext), nodeId));
    }

    void CreateBoxAndPool() {
        NKikimrBlobStorage::TConfigRequest request;

        const ui64 hostConfigId = 1;
        const ui32 drivesPerNode = 4;
        const auto pdiskType = NKikimrBlobStorage::EPDiskType::SSD;
        {
            auto *cmd = request.AddCommand();
            auto *hc = cmd->MutableDefineHostConfig();
            hc->SetHostConfigId(hostConfigId);
            for (ui32 i = 0; i < drivesPerNode; ++i) {
                auto *drive = hc->AddDrive();
                drive->SetPath(TStringBuilder() << "SectorMap:" << i << ":1000");
                drive->SetType(pdiskType);
            }
        }

        const ui64 boxId = 1;
        {
            auto *cmd = request.AddCommand();
            auto *b = cmd->MutableDefineBox();
            b->SetBoxId(boxId);
            for (const ui32 nodeId : Runtime->GetNodes()) {
                auto *host = b->AddHost();
                host->MutableKey()->SetNodeId(nodeId);
                host->SetHostConfigId(hostConfigId);
            }
        }

        const ui64 storagePoolId = 1;
        {
            auto *cmd = request.AddCommand();
            auto *sp = cmd->MutableDefineStoragePool();
            sp->SetBoxId(boxId);
            sp->SetStoragePoolId(storagePoolId);
            sp->SetName(StoragePoolName);
            sp->SetKind(StoragePoolName);
            sp->SetErasureSpecies("block-4-2");
            sp->SetVDiskKind("Default");
            sp->SetNumGroups(drivesPerNode * Settings.NodeCount);
            sp->AddPDiskFilter()->AddProperty()->SetType(pdiskType);
            if (Settings.Encryption) {
                sp->SetEncryptionMode(TBlobStorageGroupInfo::EEncryptionMode::EEM_ENC_V1);
            }
        }

        {
            auto *cmd = request.AddCommand();
            auto *sp = cmd->MutableDefineStoragePool();
            sp->SetBoxId(boxId);
            sp->SetName("virtual");
            sp->SetKind("virtual");
            sp->SetErasureSpecies("none");
            sp->SetVDiskKind("Default");
            if (Settings.Encryption) {
                sp->SetEncryptionMode(TBlobStorageGroupInfo::EEncryptionMode::EEM_ENC_V1);
            }
        }

        {
            auto *cmd = request.AddCommand();
            auto *vg = cmd->MutableAllocateVirtualGroup();
            vg->SetName("vg");
            vg->SetHiveId(Runtime->GetDomainsInfo()->GetHive());
            vg->SetStoragePoolName("virtual");
            auto *prof = vg->AddChannelProfiles();
            prof->SetStoragePoolName(StoragePoolName);
            prof->SetCount(2);
            prof = vg->AddChannelProfiles();
            prof->SetStoragePoolName(StoragePoolName);
            prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
            prof->SetCount(32);
        }

        {
            auto *cmd = request.AddCommand();
            cmd->MutableQueryBaseConfig();
        }

        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        for (const auto& status : response.GetStatus()) {
            if (status.HasBaseConfig()) {
                for (const auto& group : status.GetBaseConfig().GetGroup()) {
                    GroupOverseer.AddGroupToOversee(group.GetGroupId());
                }
            }
        }
    }

    void Sim(TDuration delta = TDuration::Zero()) {
        TInstant barrier = Runtime->GetClock() + delta;
        Runtime->Sim([&] { return Runtime->GetClock() <= barrier; });
    }

};
