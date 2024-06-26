#pragma once

#include "defs.h"

#include "node_warden_mock.h"

#include <ydb/core/driver_lib/version/version.h>
#include <ydb/core/base/blobstorage_common.h>

#include <library/cpp/testing/unittest/registar.h>

struct TEnvironmentSetup {
    std::unique_ptr<TTestActorSystem> Runtime;
    static constexpr ui32 DrivesPerNode = 5;
    const TString DomainName = "Root";
    const ui32 DomainId = 1;
    const ui64 TabletId = MakeBSControllerID();
    const ui32 GroupId = 0;
    const TString StoragePoolName = "test";
    const ui32 NumGroups = 1;
    TIntrusivePtr<NFake::TProxyDS> Group0 = MakeIntrusive<NFake::TProxyDS>();
    std::map<std::pair<ui32, ui32>, TIntrusivePtr<TPDiskMockState>> PDiskMockStates;
    TVector<TActorId> PDiskActors;
    std::set<TActorId> CommencedReplication;
    std::unordered_map<ui32, TString> Cache;

    using TIcbControlKey = std::pair<ui32, TString>;  // { nodeId, name }

    std::unordered_map<TIcbControlKey, TControlWrapper> IcbControls;

    struct TSettings {
        const ui32 NodeCount = 9;
        const bool VDiskReplPausedAtStart = false;
        const TBlobStorageGroupType Erasure = TBlobStorageGroupType::ErasureNone;
        const TNodeWardenMockActor::TSetup::TPtr NodeWardenMockSetup;
        const bool Encryption = false;
        const std::function<void(ui32, TNodeWardenConfig&)> ConfigPreprocessor;
        const std::function<void(TTestActorSystem&)> PrepareRuntime;
        const ui32 ControllerNodeId = 1;
        const bool Cache = false;
        const ui32 NumDataCenters = 0;
        const std::function<TNodeLocation(ui32)> LocationGenerator;
        const bool SetupHive = false;
        const bool SuppressCompatibilityCheck = false;
        const TFeatureFlags FeatureFlags;
        const NPDisk::EDeviceType DiskType = NPDisk::EDeviceType::DEVICE_TYPE_NVME;
        const ui64 BurstThresholdNs = 0;
        const ui32 MinHugeBlobInBytes = 0;
        const float DiskTimeAvailableScale = 1;
        const bool UseFakeConfigDispatcher = false;
        const float SlowDiskThreshold = 2;
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
                state.Reset(new TPDiskMockState(nodeId, pdiskId, cfg->PDiskGuid, ui64(10) << 40, cfg->ChunkSize,
                        Env.Settings.DiskType));
            }
            const TActorId& actorId = ctx.Register(CreatePDiskMockActor(state), TMailboxType::HTSwap, poolId);
            const TActorId& serviceId = MakeBlobStoragePDiskID(nodeId, pdiskId);
            ctx.ExecutorThread.ActorSystem->RegisterLocalService(serviceId, actorId);
            Env.PDiskActors.push_back(actorId);
        }
    };


    class TFakeConfigDispatcher : public TActor<TFakeConfigDispatcher> {
        std::unordered_set<TActorId> Subscribers;
        TActorId EdgeId;
    public:
        TFakeConfigDispatcher()
            : TActor<TFakeConfigDispatcher>(&TFakeConfigDispatcher::StateWork)
        {
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
                hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle)
                hFunc(NConsole::TEvConsole::TEvConfigNotificationResponse, Handle)
                hFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest, Handle)
            }
        }

        void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr& ev) {
            auto& items = ev->Get()->ConfigItemKinds;
            if (items.at(0) != NKikimrConsole::TConfigItem::BlobStorageConfigItem) {
                return;
            }
            Subscribers.emplace(ev->Sender);
        }

        void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
            EdgeId = ev->Sender;
            for (auto& id : Subscribers) {
                auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
                update->Record.CopyFrom(ev->Get()->Record);
                Send(id, update.Release());
            }
        }

        void Handle(NConsole::TEvConsole::TEvConfigNotificationResponse::TPtr& ev) {
            Forward(ev, EdgeId);
        }

        void Handle(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest::TPtr& ev) {
                Send(ev->Sender, MakeHolder<NConsole::TEvConsole::TEvRemoveConfigSubscriptionResponse>().Release());
        }
    };

    TEnvironmentSetup(bool vdiskReplPausedAtStart, TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureNone)
        : TEnvironmentSetup(TSettings{
            .VDiskReplPausedAtStart = vdiskReplPausedAtStart,
            .Erasure = erasure
        })
    {}

    TEnvironmentSetup(ui32 nodeCount, TNodeWardenMockActor::TSetup::TPtr nodeWardenMockSetup,
            TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureNone)
        : TEnvironmentSetup(TSettings{
            .NodeCount = nodeCount,
            .Erasure = erasure,
            .NodeWardenMockSetup = std::move(nodeWardenMockSetup),
        })
    {}

    TEnvironmentSetup(TSettings&& settings)
        : Settings(std::move(settings))
    {
        struct TSetupEnv { TSetupEnv() { TEnvironmentSetup::SetupEnv(); } };
        Singleton<TSetupEnv>();
        Initialize();
    }

    ~TEnvironmentSetup() {
        Cleanup();
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

    TInstant Now() {
        return TAppData::TimeProvider->Now();
    }

    TString GenerateRandomString(ui32 len) {
        TString res = TString::Uninitialized(len);
        char *p = res.Detach();
        char *end = p + len;
        TReallyFastRng32 rng(RandomNumber<ui64>());
        for (; p + sizeof(ui32) < end; p += sizeof(ui32)) {
            *reinterpret_cast<ui32*>(p) = rng();
        }
        for (; p < end; ++p) {
            *p = rng();
        }
        return res;
    }

    ui32 GetNumDataCenters() const {
        return Settings.NumDataCenters ? Settings.NumDataCenters :
            Settings.Erasure.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1;
    }

    std::unique_ptr<TTestActorSystem> MakeRuntime() {
        TFeatureFlags featureFlags;
        featureFlags.SetSuppressCompatibilityCheck(Settings.SuppressCompatibilityCheck);

        auto domainsInfo = MakeIntrusive<TDomainsInfo>();
        auto domain = TDomainsInfo::TDomain::ConstructEmptyDomain(DomainName, DomainId);
        domainsInfo->AddDomain(domain.Get());
        if (Settings.SetupHive) {
            domainsInfo->AddHive(MakeDefaultHiveID());
        }

        return std::make_unique<TTestActorSystem>(Settings.NodeCount, NLog::PRI_ERROR, domainsInfo, featureFlags);
    }

    void Initialize() {
        Runtime = MakeRuntime();
        if (Settings.PrepareRuntime) {
            Settings.PrepareRuntime(*Runtime);
        }
        SetupLogging();
        Runtime->Start();

        if (Settings.LocationGenerator) {
            Runtime->SetupTabletRuntime(Settings.LocationGenerator, Settings.ControllerNodeId);
        } else {
            Runtime->SetupTabletRuntime(GetNumDataCenters(), Settings.ControllerNodeId);
        }
        SetupStaticStorage();
        SetupStorage();
        SetupTablet();
    }

    void StopNode(ui32 nodeId) {
        Runtime->StopNode(nodeId);
    }

    void StartNode(ui32 nodeId) {
        Runtime->StartNode(nodeId);
        if (Settings.LocationGenerator) {
            Runtime->SetupTabletRuntime(Settings.LocationGenerator, Settings.ControllerNodeId, nodeId);
        } else {
            Runtime->SetupTabletRuntime(GetNumDataCenters(), Settings.ControllerNodeId, nodeId);
        }
        if (nodeId == Settings.ControllerNodeId) {
            SetupStaticStorage();
            SetupTablet();
        }
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

    template<typename TEvent>
    TAutoPtr<TEventHandle<TEvent>> WaitForEdgeActorEvent(const TActorId& actorId, bool termOnCapture = true,
            TInstant deadline = TInstant::Max()) {
        std::set<TActorId> ids{actorId};

        TActorId wakeup;
        if (deadline != TInstant::Max()) {
            wakeup = Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
            Runtime->Schedule(deadline, new IEventHandle(TEvents::TSystem::Wakeup, 0, wakeup, {}, nullptr, 0), nullptr,
                wakeup.NodeId());
            ids.insert(wakeup);
        }

        for (;;) {
            auto ev = Runtime->WaitForEdgeActorEvent(ids);
            if (ev->GetRecipientRewrite() == wakeup && ev->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
                Runtime->DestroyActor(wakeup);
                return nullptr;
            } else if (ev->GetTypeRewrite() == TEvent::EventType) {
                TAutoPtr<TEventHandle<TEvent>> res = reinterpret_cast<TEventHandle<TEvent>*>(ev.release());
                if (termOnCapture) {
                    Runtime->DestroyActor(actorId);
                }
                if (wakeup) {
                    Runtime->DestroyActor(wakeup);
                }
                return res;
            }
        }
    }

    NKikimrBlobStorage::TConfigResponse Invoke(const NKikimrBlobStorage::TConfigRequest& request) {
        const TActorId self = Runtime->AllocateEdgeActor(Settings.ControllerNodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
        Runtime->SendToPipe(TabletId, self, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        auto response = WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerConfigResponse>(self);
        return response->Get()->Record.GetResponse();
    }

    void SetupLogging() {
        Runtime->SetLogPriority(NKikimrServices::BS_HULLCOMP, NLog::PRI_NOTICE);
        Runtime->SetLogPriority(NKikimrServices::BS_VDISK_SCRUB, NLog::PRI_NOTICE);

        auto prio = NLog::PRI_ERROR;
        Runtime->SetLogPriority(NKikimrServices::TABLET_MAIN, prio);
        Runtime->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, prio);
        Runtime->SetLogPriority(NKikimrServices::PIPE_CLIENT, prio);
        Runtime->SetLogPriority(NKikimrServices::PIPE_SERVER, prio);
        Runtime->SetLogPriority(NKikimrServices::TABLET_RESOLVER, prio);
        Runtime->SetLogPriority(NKikimrServices::STATESTORAGE, prio);
        Runtime->SetLogPriority(NKikimrServices::BOOTSTRAPPER, prio);
        Runtime->SetLogPriority(NKikimrServices::BS_NODE, prio);

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
//            NKikimrServices::BS_PROXY_PATCH,
//            NKikimrServices::BS_PROXY_INDEXRESTOREGET,
//            NKikimrServices::BS_PROXY_STATUS,
            NActorsServices::TEST,
//            NKikimrServices::BLOB_DEPOT,
//            NKikimrServices::BLOB_DEPOT_AGENT,
//            NKikimrServices::HIVE,
//            NKikimrServices::LOCAL,
//            NActorsServices::INTERCONNECT,
//            NActorsServices::INTERCONNECT_SESSION,
//            NKikimrServices::BS_VDISK_BALANCING,
        };
        for (const auto& comp : debug) {
            Runtime->SetLogPriority(comp, NLog::PRI_DEBUG);
        }

        // toggle the flag to enable logging of actor names and events
        bool printActorNamesAndEvents = false;
        if (printActorNamesAndEvents) {
            Runtime->SetOwnLogPriority(NActors::NLog::EPrio::Info);
        }

        // Runtime->SetLogPriority(NKikimrServices::BS_REQUEST_COST, NLog::PRI_TRACE);
    }

    void SetupStaticStorage() {
        const TActorId proxyId = MakeBlobStorageProxyID(GroupId);
        for (const ui32 nodeId : Runtime->GetNodes()) {
            Runtime->RegisterService(proxyId, Runtime->Register(CreateBlobStorageGroupProxyMockActor(Group0), nodeId));
        }
    }

    void SetupStorage(ui32 targetNodeId = 0) {
        for (ui32 nodeId : Runtime->GetNodes()) {
            if (targetNodeId && nodeId != targetNodeId) {
                continue;
            }

            std::unique_ptr<IActor> warden;

            if (Settings.NodeWardenMockSetup) {
                warden.reset(new TNodeWardenMockActor(Settings.NodeWardenMockSetup));
            } else {
                auto config = MakeIntrusive<TNodeWardenConfig>(new TMockPDiskServiceFactory(*this));
                config->BlobStorageConfig.MutableServiceSet()->AddAvailabilityDomains(DomainId);
                config->VDiskReplPausedAtStart = Settings.VDiskReplPausedAtStart;
                if (Settings.ConfigPreprocessor) {
                    Settings.ConfigPreprocessor(nodeId, *config);
                }
                if (Settings.Cache) {
                    class TAccessor : public ICacheAccessor {
                        TString& Data;

                    public:
                        TAccessor(TString& data) : Data(data) {}
                        TString Read() { return Data; }
                        void Update(std::function<TString(TString)> processor) { Data = processor(Data); }
                    };
                    config->CacheAccessor = std::make_unique<TAccessor>(Cache[nodeId]);
                }
                config->FeatureFlags = Settings.FeatureFlags;

                TAppData* appData = Runtime->GetNode(nodeId)->AppData.get();

#define ADD_ICB_CONTROL(controlName, defaultVal, minVal, maxVal, currentValue) {        \
                    TControlWrapper control(defaultVal, minVal, maxVal);                \
                    appData->Icb->RegisterSharedControl(control, controlName);          \
                    control = currentValue;                                             \
                    IcbControls.insert({{nodeId, controlName}, std::move(control)});    \
                }

                if (Settings.BurstThresholdNs) {
                    ADD_ICB_CONTROL("VDiskControls.BurstThresholdNsHDD", 200'000'000, 1, 1'000'000'000'000, Settings.BurstThresholdNs);
                    ADD_ICB_CONTROL("VDiskControls.BurstThresholdNsSSD", 50'000'000,  1, 1'000'000'000'000, Settings.BurstThresholdNs);
                    ADD_ICB_CONTROL("VDiskControls.BurstThresholdNsNVME", 32'000'000,  1, 1'000'000'000'000, Settings.BurstThresholdNs);
                }
                ADD_ICB_CONTROL("VDiskControls.DiskTimeAvailableScaleHDD", 1'000, 1, 1'000'000, std::round(Settings.DiskTimeAvailableScale * 1'000));
                ADD_ICB_CONTROL("VDiskControls.DiskTimeAvailableScaleSSD", 1'000, 1, 1'000'000, std::round(Settings.DiskTimeAvailableScale * 1'000));
                ADD_ICB_CONTROL("VDiskControls.DiskTimeAvailableScaleNVME", 1'000, 1, 1'000'000, std::round(Settings.DiskTimeAvailableScale * 1'000));

                ADD_ICB_CONTROL("DSProxyControls.SlowDiskThreshold", 2'000, 1, 1'000'000, std::round(Settings.SlowDiskThreshold * 1'000));
#undef ADD_ICB_CONTROL

                {
                    auto* type = config->BlobStorageConfig.MutableVDiskPerformanceSettings()->AddVDiskTypes();
                    type->SetPDiskType(PDiskTypeToPDiskType(Settings.DiskType));
                    if (Settings.MinHugeBlobInBytes) {
                        type->SetMinHugeBlobSizeInBytes(Settings.MinHugeBlobInBytes);
                    }
                }

                warden.reset(CreateBSNodeWarden(config));
            }

            const TActorId wardenId = Runtime->Register(warden.release(), nodeId);
            Runtime->RegisterService(MakeBlobStorageNodeWardenID(nodeId), wardenId);
            if (Settings.UseFakeConfigDispatcher) {
                Runtime->RegisterService(NConsole::MakeConfigsDispatcherID(nodeId), Runtime->Register(new TFakeConfigDispatcher(), nodeId));
            }
        }
    }

    void SetupTablet() {
        struct TTabletInfo {
            ui64 TabletId;
            TTabletTypes::EType Type;
            IActor* (*Create)(const TActorId&, TTabletStorageInfo*);
            ui32 NumChannels = 3;
        };
        std::vector<TTabletInfo> tablets{
            {MakeBSControllerID(), TTabletTypes::BSController, &CreateFlatBsController},
        };

        if (const ui64 tabletId = Runtime->GetDomainsInfo()->GetHive(); tabletId != TDomainsInfo::BadTabletId) {
            tablets.push_back(TTabletInfo{tabletId, TTabletTypes::Hive, &CreateDefaultHive});
        }

        for (const TTabletInfo& tablet : tablets) {
            Runtime->CreateTestBootstrapper(
                TTestActorSystem::CreateTestTabletInfo(tablet.TabletId, tablet.Type, Settings.Erasure.GetErasure(), GroupId, tablet.NumChannels),
                tablet.Create, Settings.ControllerNodeId);

            bool working = true;
            Runtime->Sim([&] { return working; }, [&](IEventHandle& event) { working = event.GetTypeRewrite() != TEvTablet::EvBoot; });
        }

        auto localConfig = MakeIntrusive<TLocalConfig>();

        localConfig->TabletClassInfo[TTabletTypes::BlobDepot] = TLocalConfig::TTabletClassInfo(new TTabletSetupInfo(
            &NBlobDepot::CreateBlobDepot, TMailboxType::ReadAsFilled, Runtime->SYSTEM_POOL_ID, TMailboxType::ReadAsFilled,
            Runtime->SYSTEM_POOL_ID));

        auto tenantPoolConfig = MakeIntrusive<TTenantPoolConfig>(localConfig);
        tenantPoolConfig->AddStaticSlot(DomainName);

        if (Settings.SetupHive) {
            for (ui32 nodeId : Runtime->GetNodes()) {
                Runtime->RegisterService(MakeTenantPoolRootID(),
                    Runtime->Register(CreateTenantPool(tenantPoolConfig), nodeId));
                Runtime->RegisterService(NSysView::MakeSysViewServiceID(nodeId),
                    Runtime->Register(NSysView::CreateSysViewServiceForTests().Release(), nodeId));
                Runtime->Register(CreateTenantNodeEnumerationPublisher(), nodeId);
                Runtime->Register(CreateLabelsMaintainer({}), nodeId);
            }
        }
    }

    void CreateBoxAndPool(ui32 numDrivesPerNode = 0, ui32 numGroups = 0, ui32 numStorageNodes = 0,
            NKikimrBlobStorage::EPDiskType pdiskType = NKikimrBlobStorage::EPDiskType::ROT) {
        NKikimrBlobStorage::TConfigRequest request;

        auto *cmd = request.AddCommand()->MutableDefineHostConfig();
        cmd->SetHostConfigId(1);
        for (ui32 j = 0; j < (numDrivesPerNode ? numDrivesPerNode : DrivesPerNode); ++j) {
            auto *drive = cmd->AddDrive();
            drive->SetPath(Sprintf("SectorMap:%" PRIu32 ":1000", j));
            drive->SetType(pdiskType);
        }

        cmd = request.AddCommand()->MutableDefineHostConfig();
        cmd->SetHostConfigId(2);

        auto *cmd1 = request.AddCommand()->MutableDefineBox();
        cmd1->SetBoxId(1);
        ui32 index = 0;
        for (ui32 nodeId : Runtime->GetNodes()) {
            auto *host = cmd1->AddHost();
            host->MutableKey()->SetNodeId(nodeId);
            host->SetHostConfigId(numStorageNodes == 0 || index < numStorageNodes ? 1 : 2);
            ++index;
        }

        auto *cmd2 = request.AddCommand()->MutableDefineStoragePool();
        cmd2->SetBoxId(1);
        cmd2->SetStoragePoolId(1);
        cmd2->SetName(StoragePoolName);
        cmd2->SetKind(StoragePoolName);
        cmd2->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(Settings.Erasure.GetErasure()));
        cmd2->SetVDiskKind("Default");
        cmd2->SetNumGroups(numGroups ? numGroups : NumGroups);
        cmd2->AddPDiskFilter()->AddProperty()->SetType(pdiskType);
        if (Settings.Encryption) {
            cmd2->SetEncryptionMode(TBlobStorageGroupInfo::EEncryptionMode::EEM_ENC_V1);
        }

        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    void CreatePoolInBox(ui32 boxId, ui32 poolId, TString poolName) {
        NKikimrBlobStorage::TConfigRequest request;

        auto *cmd = request.AddCommand()->MutableDefineStoragePool();
        cmd->SetBoxId(boxId);
        cmd->SetStoragePoolId(poolId);
        cmd->SetName(poolName);
        cmd->SetKind(poolName);
        cmd->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(Settings.Erasure.GetErasure()));
        cmd->SetVDiskKind("Default");
        cmd->SetNumGroups(1);
        cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);
        if (Settings.Encryption) {
            cmd->SetEncryptionMode(TBlobStorageGroupInfo::EEncryptionMode::EEM_ENC_V1);
        }

        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    std::vector<ui32> GetGroups() {
        const TActorId& edge = Runtime->AllocateEdgeActor(Settings.ControllerNodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
        auto& r = ev->Record;
        auto *params = r.AddGroupParameters();
        params->MutableStoragePoolSpecifier()->SetName(StoragePoolName);
        r.SetReturnAllMatchingGroups(true);
        Runtime->SendToPipe(TabletId, edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        auto response = WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerSelectGroupsResult>(edge);
        auto& rr = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(rr.MatchingGroupsSize(), 1);
        const auto& mg = rr.GetMatchingGroups(0);
        std::vector<ui32> res;
        for (const auto& group : mg.GetGroups()) {
            res.push_back(group.GetGroupID());
        }
        return res;
    }

    NKikimrBlobStorage::TBaseConfig FetchBaseConfig() {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Invoke(request);
        UNIT_ASSERT(response.GetSuccess());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
        return response.GetStatus(0).GetBaseConfig();
    }

    TIntrusivePtr<TBlobStorageGroupInfo> GetGroupInfo(ui32 groupId) {
        const auto& baseConfig = FetchBaseConfig();
        std::map<TActorId, TVDiskID> vslotToDiskMap;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            const auto& l = vslot.GetVSlotId();
            vslotToDiskMap.emplace(MakeBlobStorageVDiskID(l.GetNodeId(), l.GetPDiskId(), l.GetVSlotId()),
                TVDiskID(TGroupId::FromProto(&vslot, &NKikimrBlobStorage::TBaseConfig_TVSlot::GetGroupId), vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
                vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()));
        }
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                std::map<TVDiskID, TActorId> vdisks;
                ui32 numFailRealms = 0;
                ui32 numFailDomainsPerFailRealm = 0;
                ui32 numVDisksPerFailDomain = 0;
                for (const auto& l : group.GetVSlotId()) {
                    const auto it = vslotToDiskMap.find(MakeBlobStorageVDiskID(l.GetNodeId(), l.GetPDiskId(), l.GetVSlotId()));
                    Y_ABORT_UNLESS(it != vslotToDiskMap.end());
                    const TVDiskID& vdiskId = it->second;
                    Y_ABORT_UNLESS(vdiskId.GroupID.GetRawId() == groupId);
                    Y_ABORT_UNLESS(vdiskId.GroupGeneration == group.GetGroupGeneration());
                    const bool inserted = vdisks.emplace(it->second, it->first).second;
                    Y_ABORT_UNLESS(inserted);
                    numFailRealms = Max<ui32>(numFailRealms, vdiskId.FailRealm + 1);
                    numFailDomainsPerFailRealm = Max<ui32>(numFailDomainsPerFailRealm, vdiskId.FailDomain + 1);
                    numVDisksPerFailDomain = Max<ui32>(numVDisksPerFailDomain, vdiskId.VDisk + 1);
                }
                Y_ABORT_UNLESS(numFailRealms * numFailDomainsPerFailRealm * numVDisksPerFailDomain == vdisks.size());
                TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType(
                    TBlobStorageGroupType::ErasureSpeciesByName(group.GetErasureSpecies())),
                    numFailRealms, numFailDomainsPerFailRealm, numVDisksPerFailDomain);
                TBlobStorageGroupInfo::TDynamicInfo dyn(TGroupId::FromProto(&group, &NKikimrBlobStorage::TBaseConfig::TGroup::GetGroupId), group.GetGroupGeneration());
                for (const auto& [vdiskId, vdiskActorId] : vdisks) {
                    dyn.PushBackActorId(vdiskActorId);
                }
                return new TBlobStorageGroupInfo(std::move(topology), std::move(dyn), "storage_pool");
            }
        }
        return nullptr;
    }

    TActorId CreateQueueActor(const TVDiskID& vdiskId, NKikimrBlobStorage::EVDiskQueueId queueId, ui32 index) {
        TBSProxyContextPtr bspctx = MakeIntrusive<TBSProxyContext>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
        auto flowRecord = MakeIntrusive<NBackpressure::TFlowRecord>();
        auto groupInfo = GetGroupInfo(vdiskId.GroupID.GetRawId());
        std::unique_ptr<IActor> actor(CreateVDiskBackpressureClient(groupInfo, vdiskId, queueId,
            MakeIntrusive<::NMonitoring::TDynamicCounters>(), bspctx,
            NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::DSProxy, index), TStringBuilder()
            << "test# " << index, 0, false, TDuration::Seconds(60), flowRecord, NMonitoring::TCountableBase::EVisibility::Private));
        const ui32 nodeId = Settings.ControllerNodeId;
        const TActorId edge = Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const TActorId actorId = Runtime->Register(actor.release(), edge, {}, {}, nodeId);
        const TInstant deadline = Runtime->GetClock() + TDuration::Minutes(1);
        for (;;) {
            auto ev = WaitForEdgeActorEvent<TEvProxyQueueState>(edge, false, deadline);
            if (!ev || ev->Get()->IsConnected) {
                Runtime->DestroyActor(edge);
                break;
            }
        }
        return actorId;
    }

    void WaitForVDiskRepl(const TActorId& actorId, const TVDiskID& vdiskId) {
        const ui32 nodeId = actorId.NodeId();
        const TActorId& edge = Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        for (;;) {
            Runtime->Send(new IEventHandle(actorId, edge, new TEvBlobStorage::TEvVStatus(vdiskId),
                IEventHandle::FlagTrackDelivery), nodeId);
            auto r = Runtime->WaitForEdgeActorEvent({edge});
            if (auto *msg = r->CastAsLocal<TEvBlobStorage::TEvVStatusResult>(); msg && msg->Record.GetReplicated()) {
                Runtime->DestroyActor(edge);
                break;
            }
            Sim(TDuration::Seconds(5));
        }
    }

    void CompactVDisk(const TActorId& actorId, bool freshOnly = false) {
        const TActorId& edge = Runtime->AllocateEdgeActor(actorId.NodeId());
        for (;;) {
            Runtime->Send(new IEventHandle(actorId, edge, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, freshOnly ?
                TEvCompactVDisk::EMode::FRESH_ONLY : TEvCompactVDisk::EMode::FULL)), actorId.NodeId());
            auto res = Runtime->WaitForEdgeActorEvent({edge});
            if (res->GetTypeRewrite() == TEvents::TSystem::Undelivered) {
                Sim(TDuration::Seconds(5));
            } else {
                Y_ABORT_UNLESS(res->GetTypeRewrite() == TEvBlobStorage::EvCompactVDiskResult);
                Runtime->DestroyActor(edge);
                return;
            }
        }
    }

    void Sim(TDuration delta = TDuration::Zero()) {
        TInstant barrier = Runtime->GetClock() + delta;
        Runtime->Sim([&] { return Runtime->GetClock() <= barrier; });
    }

    TString Dump(const TActorId& vdiskActorId, const TVDiskID& vdiskId) {
        const TActorId& edge = Runtime->AllocateEdgeActor(vdiskActorId.NodeId(), __FILE__, __LINE__);
        Runtime->Send(new IEventHandle(vdiskActorId, edge, new TEvBlobStorage::TEvVDbStat(vdiskId,
            NKikimrBlobStorage::EDbStatAction::DumpDb, NKikimrBlobStorage::EDbStatType::StatLogoBlobs,
            true)), edge.NodeId());
        return WaitForEdgeActorEvent<TEvBlobStorage::TEvVDbStatResult>(edge)->Get()->Record.GetData();
    }

    void WithQueueId(const TVDiskID& vdiskId, NKikimrBlobStorage::EVDiskQueueId queue, std::function<void(TActorId)> action) {
        const TActorId& queueId = CreateQueueActor(vdiskId, queue, 1000);
        action(queueId);
        Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, queueId, {}, nullptr, 0), queueId.NodeId());
    }

    void CheckBlob(const TActorId& vdiskActorId, const TVDiskID& vdiskId, const TLogoBlobID& blobId, const TString& part,
            NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
            const TActorId& edge = Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId,
                TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                Nothing(), {{blobId, 1u, ui32(part.size() - 2)}}).release()), queueId.NodeId());
            auto r = WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
            {
                auto& record = r->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                const auto& res = record.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), status, Dump(vdiskActorId, vdiskId));
                if (status == NKikimrProto::OK) {
                    UNIT_ASSERT_VALUES_EQUAL(r->Get()->GetBlobData(res).ConvertToString(), part.substr(1, part.size() - 2));
                }
            }

            Runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(vdiskId,
                TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                Nothing(), {{blobId.FullID(), 0, 0}}).release()), queueId.NodeId());
            r = WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
            {
                auto& record = r->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                const auto& res = record.GetResult(0);
                UNIT_ASSERT(!r->Get()->HasBlob(res));
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(res.GetBlobID()), blobId.FullID());
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), status);
            }
        });
    }

    void PutBlob(const TVDiskID& vdiskId, const TLogoBlobID& blobId, const TString& part) {
        WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            const TActorId& edge = Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVPut(blobId, TRope(part), vdiskId, false, nullptr,
                TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog)), queueId.NodeId());
            auto r = WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge);

            auto& record = r->Get()->Record;
            Cerr << "*** PUT BLOB " << blobId << " TO " << vdiskId << " FINISHED WITH "
                << NKikimrProto::EReplyStatus_Name(record.GetStatus()) << " ***" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
        });
    }

    void PutBlob(const ui32 groupId, const TLogoBlobID& blobId, const TString& part) {
        TActorId edge = Runtime->AllocateEdgeActor(Settings.ControllerNodeId);
        Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, part, TInstant::Max(),
                NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticMaxThroughput));
        });
        auto res = WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge);
        Y_ABORT_UNLESS(res->Get()->Status == NKikimrProto::OK);
    }

    void CommenceReplication() {
        for (ui32 groupId : GetGroups()) {
            auto info = GetGroupInfo(groupId);
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TActorId& serviceId = info->GetActorId(i);
                if (!CommencedReplication.insert(serviceId).second) {
                    continue;
                }
                while (!Runtime->Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0, serviceId, {}, nullptr, 0), serviceId.NodeId())) {
                    Sim(TDuration::Seconds(1)); // VDisk may be not created yet
                }
                WaitForVDiskRepl(serviceId, info->GetVDiskId(i));
            }
        }
    }

    void WaitForSync(const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TLogoBlobID& blobId) {
        std::map<TVDiskID, TActorId> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TVDiskID& vdiskId = info->GetVDiskId(i);
            queues.emplace(vdiskId, CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000));
        }
        for (;;) {
            ui32 numDone = 0;
            for (const auto& [vdiskId, queueId] : queues) {
                const ui32 nodeId = queueId.NodeId();
                const TActorId& edge = Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
                Runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                    vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, {blobId.FullID()}).release()), nodeId);
                auto ev = WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                const auto& record = ev->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                const auto& result = record.GetResult(0);
                numDone += result.HasIngress();
            }
            Cerr << "numDone# " << numDone << Endl;
            if (numDone >= info->Type.BlobSubgroupSize()) {
                break;
            }
            Sim(TDuration::Seconds(10));
        }
        for (const auto& [vdiskId, queueId] : queues) {
            Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, queueId, {}, nullptr, 0), queueId.NodeId());
        }
    }

    void UpdateSettings(bool selfHeal, bool donorMode, bool groupLayoutSanitizer = false) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand();
        auto *us = cmd->MutableUpdateSettings();
        us->AddEnableSelfHeal(selfHeal);
        us->AddEnableDonorMode(donorMode);
        us->AddEnableGroupLayoutSanitizer(groupLayoutSanitizer);
        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    void EnableDonorMode() {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableEnableDonorMode()->SetEnable(true);
        auto response = Invoke(request);
        UNIT_ASSERT(response.GetSuccess());
    }

    void FillVSlotId(ui32 nodeId, ui32 pdiskId, ui32 vslotId, NKikimrBlobStorage::TVSlotId* vslot) {
        vslot->SetNodeId(nodeId);
        vslot->SetPDiskId(pdiskId);
        vslot->SetVSlotId(vslotId);
    }

    void SetVDiskReadOnly(ui32 nodeId, ui32 pdiskId, ui32 vslotId, const TVDiskID& vdiskId, bool value) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *roCmd = request.AddCommand()->MutableSetVDiskReadOnly();
        FillVSlotId(nodeId, pdiskId, vslotId, roCmd->MutableVSlotId());
        VDiskIDFromVDiskID(vdiskId, roCmd->MutableVDiskId());
        roCmd->SetValue(value);
        Cerr << "Invoking SetVDiskReadOnly for vdisk " << vdiskId.ToString() << Endl;
        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    void UpdateDriveStatus(ui32 nodeId, ui32 pdiskId, NKikimrBlobStorage::EDriveStatus status,
            NKikimrBlobStorage::EDecommitStatus decommitStatus) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand();
        auto *ds = cmd->MutableUpdateDriveStatus();
        ds->MutableHostKey()->SetNodeId(nodeId);
        ds->SetPDiskId(pdiskId);
        ds->SetStatus(status);
        ds->SetDecommitStatus(decommitStatus);
        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    void SetScrubPeriodicity(TDuration periodicity) {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableSetScrubPeriodicity()->SetScrubPeriodicity(periodicity.Seconds());
        auto response = Invoke(request);
        UNIT_ASSERT(response.GetSuccess());
    }

    void SettlePDisk(const TActorId& vdiskActorId) {
        ui32 nodeId, pdiskId;
        std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);
        for (const auto& status : {NKikimrBlobStorage::EDriveStatus::BROKEN, NKikimrBlobStorage::EDriveStatus::ACTIVE}) {
            for (;;) {
                NKikimrBlobStorage::TConfigRequest request;
                auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
                cmd->MutableHostKey()->SetNodeId(nodeId);
                cmd->SetPDiskId(pdiskId);
                cmd->SetStatus(status);
                auto response = Invoke(request);
                if (response.GetSuccess()) {
                    break;
                }
                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
                Sim(TDuration::Seconds(5));
            }
        }
        Sim(TDuration::Seconds(15));
    }

    void Wipe(ui32 nodeId, ui32 pdiskId, ui32 vslotId, const TVDiskID& vdiskId) {
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreGroupFailModelChecks(true);
        request.SetIgnoreDegradedGroupsChecks(true);
        request.SetIgnoreDisintegratedGroupsChecks(true);
        auto *cmd = request.AddCommand();
        auto *wipe = cmd->MutableWipeVDisk();
        auto *vslot = wipe->MutableVSlotId();
        vslot->SetNodeId(nodeId);
        vslot->SetPDiskId(pdiskId);
        vslot->SetVSlotId(vslotId);
        VDiskIDFromVDiskID(vdiskId, wipe->MutableVDiskId());
        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    void WaitForVDiskToGetRunning(const TVDiskID& vdiskId, TActorId actorId) {
        for (;;) {
            const TActorId& edge = Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
            auto ev = std::make_unique<TEvBlobStorage::TEvVStatus>(vdiskId);
            TString request = ev->ToString();
            Runtime->Send(new IEventHandle(actorId, edge, ev.release(), IEventHandle::FlagTrackDelivery), edge.NodeId());
            auto res = Runtime->WaitForEdgeActorEvent({edge});
            if (auto *msg = res->CastAsLocal<TEvBlobStorage::TEvVStatusResult>()) {
                const auto& record = msg->Record;
                if (record.GetStatus() == NKikimrProto::OK && record.GetJoinedGroup() && record.GetReplicated()) {
                    break;
                }
            } else if (auto *msg = res->CastAsLocal<TEvents::TEvUndelivered>()) {
                Y_ABORT_UNLESS(msg->SourceType == TEvBlobStorage::EvVStatus);
            } else {
                Y_ABORT();
            }

            // sleep for a while
            {
                const TActorId& edge = Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
                Runtime->Schedule(TDuration::Minutes(1), new IEventHandle(TEvents::TSystem::Wakeup, 0, edge, {}, nullptr, 0), nullptr, edge.NodeId());
                WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
            }
        }
    }

    template<typename TResult, typename TFactory>
    std::unique_ptr<TResult> SyncQueryFactory(const TActorId& actorId, TFactory&& factory, bool checkUndelivered = true) {
        const TActorId& edge = Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
        for (;;) {
            std::unique_ptr<typename std::invoke_result_t<TFactory>::element_type> ev(factory());
            Runtime->Send(new IEventHandle(actorId, edge, ev.release(), IEventHandle::FlagTrackDelivery), edge.NodeId());
            auto res = Runtime->WaitForEdgeActorEvent({edge});
            if (auto *msg = res->CastAsLocal<TEvents::TEvUndelivered>()) {
                UNIT_ASSERT(checkUndelivered);
                Sim(TDuration::Seconds(5));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(res->Type, TResult::EventType);
                Runtime->DestroyActor(edge);
                return std::unique_ptr<TResult>(static_cast<TResult*>(res->ReleaseBase().Release()));
            }
        }
    }

    template<typename TResult, typename TQuery, typename... TArgs>
    std::unique_ptr<TResult> SyncQuery(const TActorId& actorId, TArgs&&... args) {
        return SyncQueryFactory<TResult>(actorId, [&] { return std::make_unique<TQuery>(args...); });
    }

    ui64 AggregateVDiskCounters(TString storagePool, ui32 nodesCount, ui32 groupSize, ui32 groupId,
            const std::vector<ui32>& pdiskLayout, TString subsystem, TString counter, bool derivative = false) {
        ui64 ctr = 0;

        for (ui32 nodeId = 1; nodeId <= nodesCount; ++nodeId) {
            auto* appData = Runtime->GetNode(nodeId)->AppData.get();
            for (ui32 i = 0; i < groupSize; ++i) {
                TStringStream ss;
                ss << LeftPad(i, 2, '0');
                TString orderNumber = ss.Str();
                ss.Clear();
                ss << LeftPad(pdiskLayout[i], 9, '0');
                TString pdisk = ss.Str();
                ctr += GetServiceCounters(appData->Counters, "vdisks")->
                        GetSubgroup("storagePool", storagePool)->
                        GetSubgroup("group", std::to_string(groupId))->
                        GetSubgroup("orderNumber", orderNumber)->
                        GetSubgroup("pdisk", pdisk)->
                        GetSubgroup("media", "rot")->
                        GetSubgroup("subsystem", subsystem)->
                        GetCounter(counter, derivative)->Val();
            }
        }
        return ctr;
    };

    void SetIcbControl(ui32 nodeId, TString controlName, ui64 value) {
        if (nodeId == 0) {
            for (nodeId = 1; nodeId <= Settings.NodeCount; ++nodeId) {
                auto it = IcbControls.find({nodeId, controlName});
                Y_ABORT_UNLESS(it != IcbControls.end());
                it->second = value;
            }
        } else {
            auto it = IcbControls.find({nodeId, controlName});
            Y_ABORT_UNLESS(it != IcbControls.end());
            it->second = value;
        }
    }

};
