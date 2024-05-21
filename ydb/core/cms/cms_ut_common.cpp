#include "cms_impl.h"
#include "cms_ut_common.h"
#include "sentinel.h"
#include "ut_helpers.h"

#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/subst.h>

#include <memory>

const bool STRAND_PDISK = true;

#ifndef NDEBUG
const bool ENABLE_DETAILED_CMS_LOG = true;
#else
const bool ENABLE_DETAILED_CMS_LOG = false;
#endif

namespace NKikimr {
namespace NCmsTest {

using namespace NCms;
using namespace NConsole;
using namespace NNodeWhiteboard;
using namespace NKikimrWhiteboard;
using namespace NKikimrCms;
using namespace NKikimrBlobStorage;

void TFakeNodeWhiteboardService::Handle(TEvConfigsDispatcher::TEvGetConfigRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    Y_UNUSED(ev);
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableBootstrapConfig()->CopyFrom(BootstrapConfig);
    auto resp = MakeHolder<TEvConfigsDispatcher::TEvGetConfigResponse>();
    resp->Config = std::make_shared<NKikimrConfig::TAppConfig>(appConfig);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TFakeNodeWhiteboardService::Handle(TEvBlobStorage::TEvControllerConfigRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    auto &rec = ev->Get()->Record;
    auto resp = MakeHolder<TEvBlobStorage::TEvControllerConfigResponse>();
    if (rec.GetRequest().CommandSize() && rec.GetRequest().GetCommand(0).HasQueryBaseConfig()) {
        resp->Record.CopyFrom(Config);
    } else if (rec.GetRequest().CommandSize() && rec.GetRequest().GetCommand(0).HasReadDriveStatus()) {
        auto &drive = rec.GetRequest().GetCommand(0).GetReadDriveStatus();
        auto &status = *resp->Record.MutableResponse()->AddStatus();
        resp->Record.MutableResponse()->SetSuccess(true);
        status.SetSuccess(true);
        auto &driveStatus = *status.AddDriveStatus();
        driveStatus.MutableHostKey()->SetFqdn(drive.GetHostKey().GetFqdn());
        driveStatus.MutableHostKey()->SetIcPort(drive.GetHostKey().GetIcPort());
        driveStatus.SetPath(drive.GetPath());
        driveStatus.SetStatus(NKikimrBlobStorage::ACTIVE);
    } else if (rec.GetRequest().CommandSize() && rec.GetRequest().GetCommand(0).HasUpdateDriveStatus()) {
        // assume that all commands are UpdateDriveStatus
        if (NoisyBSCPipe && ++NoisyBSCPipeCounter % 3) {
            ctx.Send(ev->Sender, new TEvSentinel::TEvBSCPipeDisconnected, 0);
            return;
        }
        bool success = true;
        for (ui32 i = 0; i < rec.GetRequest().CommandSize(); ++i) {
            const auto &cmd = rec.GetRequest().GetCommand(i).GetUpdateDriveStatus();
            const auto id = NCms::TPDiskID(cmd.GetHostKey().GetNodeId(), cmd.GetPDiskId());
            if (auto& pattern = BSControllerResponsePatterns[id]; !pattern.empty()) {
                success = success && pattern[0];
                resp->Record.MutableResponse()->AddStatus()->SetSuccess(pattern[0]);
                pattern.erase(pattern.begin());
                if (!success) {
                    break;
                }
            } else {
                resp->Record.MutableResponse()->AddStatus()->SetSuccess(true);
            }
        }
        resp->Record.MutableResponse()->SetSuccess(success);
    }
    ctx.Send(ev->Sender, std::move(resp), 0, ev->Cookie);
}

void TFakeNodeWhiteboardService::Handle(TEvWhiteboard::TEvTabletStateRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    const auto &node = Info[ctx.SelfID.NodeId()];
    if (!node.Connected) {
        ctx.Send(ev->Sender, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), TEvents::TEvUndelivered::Disconnected), 0, ev->Cookie);
        return;
    }
    TAutoPtr<TEvWhiteboard::TEvTabletStateResponse> response = new TEvWhiteboard::TEvTabletStateResponse();
    auto& record = response->Record;
    for (const auto& pr : node.TabletStateInfo) {
        NKikimrWhiteboard::TTabletStateInfo &tabletStateInfo = *record.AddTabletStateInfo();
        tabletStateInfo.CopyFrom(pr.second);
    }
    response->Record.SetResponseTime(ctx.Now().MilliSeconds());
    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TFakeNodeWhiteboardService::Handle(TEvWhiteboard::TEvNodeStateRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    const auto &node = Info[ctx.SelfID.NodeId()];
    if (!node.Connected) {
        ctx.Send(ev->Sender, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), TEvents::TEvUndelivered::Disconnected), 0, ev->Cookie);
        return;
    }
    TAutoPtr<TEvWhiteboard::TEvNodeStateResponse> response = new TEvWhiteboard::TEvNodeStateResponse();
    auto& record = response->Record;
    for (const auto& pr : node.NodeStateInfo) {
        NKikimrWhiteboard::TNodeStateInfo &nodeStateInfo = *record.AddNodeStateInfo();
        nodeStateInfo.CopyFrom(pr.second);
    }
    response->Record.SetResponseTime(ctx.Now().MilliSeconds());
    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TFakeNodeWhiteboardService::Handle(TEvWhiteboard::TEvPDiskStateRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    const auto &node = Info[ctx.SelfID.NodeId()];
    if (!node.Connected) {
        ctx.Send(ev->Sender, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), TEvents::TEvUndelivered::Disconnected), 0, ev->Cookie);
        return;
    }
    TAutoPtr<TEvWhiteboard::TEvPDiskStateResponse> response = new TEvWhiteboard::TEvPDiskStateResponse();
    auto& record = response->Record;
    for (const auto& pr : node.PDiskStateInfo) {
        NKikimrWhiteboard::TPDiskStateInfo &pDiskStateInfo = *record.AddPDiskStateInfo();
        pDiskStateInfo.CopyFrom(pr.second);
    }
    response->Record.SetResponseTime(ctx.Now().MilliSeconds());
    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TFakeNodeWhiteboardService::Handle(TEvWhiteboard::TEvVDiskStateRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    const auto &node = Info[ctx.SelfID.NodeId()];
    if (!node.Connected) {
        ctx.Send(ev->Sender, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), TEvents::TEvUndelivered::Disconnected), 0, ev->Cookie);
        return;
    }
    TAutoPtr<TEvWhiteboard::TEvVDiskStateResponse> response = new TEvWhiteboard::TEvVDiskStateResponse();
    auto& record = response->Record;
    for (const auto& pr : node.VDiskStateInfo) {
        NKikimrWhiteboard::TVDiskStateInfo &vDiskStateInfo = *record.AddVDiskStateInfo();
        vDiskStateInfo.CopyFrom(pr.second);
    }
    response->Record.SetResponseTime(ctx.Now().MilliSeconds());
    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TFakeNodeWhiteboardService::Handle(TEvWhiteboard::TEvSystemStateRequest::TPtr &ev,
                                        const TActorContext &ctx)
{
    TGuard<TMutex> guard(Mutex);
    const auto &node = Info[ctx.SelfID.NodeId()];
    if (!node.Connected) {
        ctx.Send(ev->Sender, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), TEvents::TEvUndelivered::Disconnected), 0, ev->Cookie);
        return;
    }
    TAutoPtr<TEvWhiteboard::TEvSystemStateResponse> response = new TEvWhiteboard::TEvSystemStateResponse();
    auto& record = response->Record;
    NKikimrWhiteboard::TSystemStateInfo &systemStateInfo = *record.AddSystemStateInfo();
    systemStateInfo.CopyFrom(node.SystemStateInfo);
    response->Record.SetResponseTime(ctx.Now().MilliSeconds());
    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

namespace {

class TFakeTenantPool : public TActorBootstrapped<TFakeTenantPool> {
public:
    TVector<TString> Tenants;

    TFakeTenantPool(const TVector<TString> &tenants)
        : Tenants(tenants)
    {
    }

    void Bootstrap(const TActorContext &ctx)
    {
        Y_UNUSED(ctx);
        Become(&TFakeTenantPool::StateWork);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTenantPool::TEvGetStatus, Handle);
        }
    }

    void Handle(TEvTenantPool::TEvGetStatus::TPtr &ev, const TActorContext &ctx)
    {
        auto *resp = new TEvTenantPool::TEvTenantPoolStatus;
        for (auto tenant : Tenants) {
            NKikimrTenantPool::TSlotStatus& slotStatus = *resp->Record.AddSlots();
            slotStatus.SetId("slot-1");
            slotStatus.SetType("static");
            slotStatus.SetAssignedTenant(tenant);
            slotStatus.SetLabel("static");
        }
        ctx.Send(ev->Sender, resp, 0, ev->Cookie);
    }
};

void GenerateExtendedInfo(TTestActorRuntime &runtime, NKikimrBlobStorage::TBaseConfig *config,
        ui32 pdisks, ui32 vdiskPerPdisk = 4, const TNodeTenantsMap &tenants = {}, bool useMirror3dcErasure = false)
{
    ui32 numNodes = runtime.GetNodeCount();
    ui32 numNodeGroups = pdisks * vdiskPerPdisk;
    ui32 numGroups;

    if (numNodes < 9)
        useMirror3dcErasure = false;

    if (useMirror3dcErasure)
        numGroups = numNodes * numNodeGroups / 9;
    else if (numNodes >= 8)
        numGroups = numNodes * numNodeGroups / 8;
    else
        numGroups = numNodes * numNodeGroups;

    auto now = runtime.GetTimeProvider()->Now();
    for (ui32 groupId = 0; groupId < numGroups; ++groupId) {
        auto &group = *config->AddGroup();
        group.SetGroupId(groupId);
        group.SetGroupGeneration(1);
        if (useMirror3dcErasure)
            group.SetErasureSpecies("mirror-3-dc");
        else if (numNodes >= 8)
            group.SetErasureSpecies("block-4-2");
        else
            group.SetErasureSpecies("none");
    }

    for (ui32 nodeIndex = 0; nodeIndex < numNodes; ++nodeIndex) {
        ui32 nodeId = runtime.GetNodeId(nodeIndex);

        auto ret = TFakeNodeWhiteboardService::Info.emplace(nodeId, TFakeNodeInfo());
        auto &node = ret.first->second;
        if (ret.second) {
            node.SystemStateInfo.SetVersion(ToString(GetProgramSvnRevision()));
            node.SystemStateInfo.SetStartTime(now.GetValue());
            node.SystemStateInfo.SetChangeTime(now.GetValue());
        }

        if (tenants.contains(nodeIndex)) {
            node.SystemStateInfo.AddRoles("Tenant");
            continue;
        } else {
            node.SystemStateInfo.AddRoles("Storage");
        }

        ui32 groupShift = (nodeIndex / 8) * pdisks * vdiskPerPdisk;
        if (numNodes < 8)
            groupShift = nodeIndex * numNodeGroups;
        if (useMirror3dcErasure)
            groupShift = (nodeIndex / 9) * pdisks * vdiskPerPdisk;

        for (ui32 pdiskIndex = 0; pdiskIndex < pdisks; ++pdiskIndex) {
            auto pdiskId = nodeId * pdisks + pdiskIndex;
            auto &pdisk = node.PDiskStateInfo[pdiskId];
            pdisk.SetPDiskId(pdiskId);
            pdisk.SetCreateTime(now.GetValue());
            pdisk.SetChangeTime(now.GetValue());
            pdisk.SetPath("/pdisk.data");
            pdisk.SetGuid(1);
            pdisk.SetAvailableSize(100ULL << 30);
            pdisk.SetTotalSize(200ULL << 30);
            pdisk.SetState(NKikimrBlobStorage::TPDiskState::Normal);

            auto &pdiskConfig = *config->AddPDisk();
            pdiskConfig.SetNodeId(nodeId);
            pdiskConfig.SetPDiskId(pdiskId);
            pdiskConfig.SetPath("/pdisk.data");
            pdiskConfig.SetGuid(1);
            pdiskConfig.SetDriveStatus(NKikimrBlobStorage::ACTIVE);

            if (node.VDisksMoved) {
                continue;
            }

            for (ui8 vdiskIndex = 0; vdiskIndex < vdiskPerPdisk; ++vdiskIndex) {
                ui32 vdiskId = pdiskIndex * vdiskPerPdisk + vdiskIndex;
                ui32 groupId = groupShift + vdiskId;
                ui32 failRealm = 0;
                if (useMirror3dcErasure)
                    failRealm = (nodeIndex % 9) / 3;

                TVDiskID id = {(ui8)groupId, 1, (ui8)failRealm, (ui8)(nodeIndex % 8), (ui8)0};

                auto &vdisk = node.VDiskStateInfo[id];
                VDiskIDFromVDiskID(id, vdisk.MutableVDiskId());
                vdisk.SetCreateTime(now.GetValue());
                vdisk.SetChangeTime(now.GetValue());
                vdisk.SetPDiskId(pdiskId);
                vdisk.SetVDiskSlotId(vdiskIndex);
                vdisk.SetVDiskState(NKikimrWhiteboard::OK);
                vdisk.SetReplicated(true);

                auto &vdiskConfig = *config->AddVSlot();
                vdiskConfig.MutableVSlotId()->SetNodeId(nodeId);
                vdiskConfig.MutableVSlotId()->SetPDiskId(pdiskId);
                vdiskConfig.MutableVSlotId()->SetVSlotId(1000 + vdiskIndex);
                vdiskConfig.SetGroupId(groupId);
                vdiskConfig.SetGroupGeneration(1);
                vdiskConfig.SetFailRealmIdx(failRealm);
                vdiskConfig.SetFailDomainIdx(nodeIndex % 8);

                config->MutableGroup(groupId)->AddVSlotId()
                    ->CopyFrom(vdiskConfig.GetVSlotId());
            }
        }
    }
}

IActor *CreateFakeNodeWhiteboardService()
{
    return new TFakeNodeWhiteboardService;
}

static bool IsTabletActiveEvent(IEventHandle& ev) {
    if (ev.GetTypeRewrite() == NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate) {
        if (ev.Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>()->Record.GetState()
            == NKikimrWhiteboard::TTabletStateInfo::Active) {
            return true;
        }
    }
    return false;
}


inline void AddTablet(NKikimrConfig::TBootstrap::ETabletType type,
                      const TVector<ui32> &nodes,
                      NKikimrConfig::TBootstrap &config)
{
    auto &tablet = *config.AddTablet();
    tablet.SetType(type);
    for (ui32 node : nodes)
        tablet.AddNode(node);
}

static NKikimrConfig::TBootstrap GenerateBootstrapConfig(TTestActorRuntime &runtime,
                                                         const ui32 nodesCount,
                                                         const TNodeTenantsMap &tenants) {
    NKikimrConfig::TBootstrap res;

    TVector<ui32> nodes;
    nodes.reserve(nodesCount);
    for (ui32 nodeIndex = 0; nodeIndex < nodesCount; ++nodeIndex) {
        if (tenants.contains(nodeIndex))
            continue;

        ui32 nodeId = runtime.GetNodeId(nodeIndex);
        nodes.push_back(nodeId);
    }

    AddTablet(NKikimrConfig::TBootstrap::FLAT_BS_CONTROLLER, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::FLAT_SCHEMESHARD, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::FLAT_TX_COORDINATOR, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::TX_MEDIATOR, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::TX_ALLOCATOR, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::CONSOLE, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::CMS, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::NODE_BROKER, nodes, res);
    AddTablet(NKikimrConfig::TBootstrap::TENANT_SLOT_BROKER, nodes, res);

    return res;
}

static void SetupServices(TTestActorRuntime &runtime, const TTestEnvOpts &options) {
    const ui32 domainsNum = 1;
    const ui32 disksInDomain = 1;

    TAppPrepare app;

    { // setup domain info
        app.ClearDomainsAndHive();
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
    }
    { // setup channel profiles
        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;
        channelProfiles->Profiles.emplace_back();
        TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
        for (ui32 channelIdx = 0; channelIdx < 3; ++channelIdx) {
            profile.Channels.push_back(
                                       TChannelProfiles::TProfile::TChannel(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default));
        }
        app.SetChannels(std::move(channelProfiles));
    }

    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        TString staticConfig("AvailabilityDomains: 0 "
                             "PDisks { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 Path: \"pdisk0.dat\" }"
                             "VDisks { VDiskID { GroupID: 0 GroupGeneration: 1 Ring: 0 Domain: 0 VDisk: 0 }"
                             "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 0 }"
                             "}"
                             "Groups { GroupID: 0 GroupGeneration: 1 ErasureSpecies: 0 "// None
                             "    Rings {"
                             "        FailDomains { VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 0 PDiskGuid: 1 } }"
                             "    }"
                             "}");

        SubstGlobal(staticConfig, "$Node1", Sprintf("%" PRIu32, runtime.GetNodeId(0)));

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig =
            new TNodeWardenConfig(STRAND_PDISK && !runtime.IsRealThreads()
                                  ? static_cast<IPDiskServiceFactory*>(new TStrandedPDiskServiceFactory(runtime))
                                  : static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory()));
        google::protobuf::TextFormat::ParseFromString(staticConfig, nodeWardenConfig->BlobStorageConfig.MutableServiceSet());

        if (nodeIndex == 0) {
            TString pDiskPath;
            TIntrusivePtr<NPDisk::TSectorMap> sectorMap;
            ui64 pDiskSize = 32ull << 30ull;
            ui64 pDiskChunkSize = 32u << 20u;
            if (true /*in memory*/) {
                pDiskPath = "/TString/pdisk0.dat";
                auto& existing = nodeWardenConfig->SectorMaps[pDiskPath];
                if (existing && existing->DeviceSize == pDiskSize) {
                    sectorMap = existing;
                } else {
                    sectorMap.Reset(new NPDisk::TSectorMap(pDiskSize));
                    nodeWardenConfig->SectorMaps[pDiskPath] = sectorMap;
                }
            } else {
                static TTempDir tempDir;
                pDiskPath = tempDir() + "/pdisk0.dat";
            }
            nodeWardenConfig->BlobStorageConfig.MutableServiceSet()->MutablePDisks(0)->SetPath(pDiskPath);
            ui64 pDiskGuid = 1;
            static ui64 iteration = 0;
            ++iteration;
            FormatPDisk(pDiskPath,
                        pDiskSize,
                        4 << 10,
                        pDiskChunkSize,
                        pDiskGuid,
                        0x1234567890 + iteration,
                        0x4567890123 + iteration,
                        0x7890123456 + iteration,
                        NPDisk::YdbDefaultPDiskSequence,
                        TString(""),
                        false,
                        false,
                        sectorMap,
                        false);
        }

        SetupBSNodeWarden(runtime, nodeIndex, nodeWardenConfig);
        SetupTabletResolver(runtime, nodeIndex);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableBootstrapConfig()->CopyFrom(TFakeNodeWhiteboardService::BootstrapConfig);

        // fake NodeWhiteBoard
        runtime.AddLocalService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(runtime.GetNodeId(nodeIndex)),
                                TActorSetupCmd(CreateFakeNodeWhiteboardService(), TMailboxType::Simple, 0), nodeIndex);
        TVector<TString> nodeTenants;
        if (options.Tenants.contains(nodeIndex))
            nodeTenants = options.Tenants.at(nodeIndex);
        runtime.AddLocalService(MakeTenantPoolID(runtime.GetNodeId(nodeIndex)),
                                TActorSetupCmd(new TFakeTenantPool(nodeTenants), TMailboxType::Simple, 0), nodeIndex);
    }

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableBootstrapConfig()->CopyFrom(TFakeNodeWhiteboardService::BootstrapConfig);
    appConfig.MutableFeatureFlags()->SetEnableCMSRequestPriorities(options.EnableCMSRequestPriorities);
    runtime.AddLocalService(
        MakeConfigsDispatcherID(
            runtime.GetNodeId(0)),
            TActorSetupCmd(
                CreateConfigsDispatcher(
                    NKikimr::NConfig::TConfigsDispatcherInitInfo {
                        .InitialConfig = appConfig,
                    }),
                TMailboxType::Simple,
                0
            ),
        0);

    runtime.Initialize(app.Unwrap());
    auto dnsConfig = new TDynamicNameserviceConfig();
    dnsConfig->MaxStaticNodeId = 1000;
    dnsConfig->MinDynamicNodeId = 1001;
    dnsConfig->MaxDynamicNodeId = 2000;
    runtime.GetAppData().DynamicNameserviceConfig = dnsConfig;
    runtime.GetAppData().DisableCheckingSysNodesCms = true;
    runtime.GetAppData().BootstrapConfig = TFakeNodeWhiteboardService::BootstrapConfig;

    NKikimrCms::TCmsConfig cmsConfig;
    cmsConfig.MutableSentinelConfig()->SetEnable(options.EnableSentinel);
    runtime.GetAppData().DefaultCmsConfig = MakeHolder<NKikimrCms::TCmsConfig>(cmsConfig);

    if (!runtime.IsRealThreads()) {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvLocalRecoveryDone,
                                                                             domainsNum * disksInDomain));
        runtime.DispatchEvents(options);
    }

    auto cid = CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeConsoleID(), TTabletTypes::Console),
                     &NConsole::CreateConsole);
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController),
                     &CreateFlatBsController);

    auto aid = CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeCmsID(), TTabletTypes::Cms), &CreateCms);
    runtime.EnableScheduleForActor(aid, true);
    runtime.EnableScheduleForActor(cid, true);
}

} // anonymous namespace

TCmsTestEnv::TCmsTestEnv(const TTestEnvOpts &options)
        : TTestBasicRuntime(options.NodeCount, options.DataCenterCount, false)
        , CmsId(MakeCmsID())
{
    TFakeNodeWhiteboardService::Config.MutableResponse()->SetSuccess(true);
    TFakeNodeWhiteboardService::Config.MutableResponse()->ClearStatus();
    auto &status = *TFakeNodeWhiteboardService::Config.MutableResponse()->AddStatus();
    status.SetSuccess(true);
    auto *config = status.MutableBaseConfig();

    TFakeNodeWhiteboardService::BootstrapConfig = GenerateBootstrapConfig(*this, options.NodeCount, options.Tenants);

    TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
    TFakeNodeWhiteboardService::Info.clear();
    GenerateExtendedInfo(*this, config, options.VDisks, 4, options.Tenants, options.UseMirror3dcErasure);

    SetObserverFunc([](TAutoPtr<IEventHandle> &event) -> auto {
        if (event->GetTypeRewrite() == TEvBlobStorage::EvControllerConfigRequest
            || event->Type == TEvBlobStorage::EvControllerConfigRequest
            || event->GetTypeRewrite() == TEvConfigsDispatcher::EvGetConfigRequest) {
            auto fakeId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(event->Recipient.NodeId());
            if (event->Recipient != fakeId)
                event = IEventHandle::Forward(std::move(event), fakeId);
        }

        return TTestActorRuntime::EEventAction::PROCESS;
    });

    using namespace NMalloc;
    TMallocInfo mallocInfo = MallocInfo();
    mallocInfo.SetParam("FillMemoryOnAllocation", "false");
    SetupLogging();

    for (ui32 nodeIndex = 0; nodeIndex < GetNodeCount(); ++nodeIndex) {
        if (options.NRings > 1) {
            SetupCustomStateStorage(*this, options.NToSelect, options.NRings, options.RingSize);
        } else {
            SetupStateStorage(*this, nodeIndex);
        }
    }
    SetupServices(*this, options);

    Sender = AllocateEdgeActor();
    ClientId = TActorId();

    NKikimrCms::TCmsConfig cmsConfig;
    cmsConfig.MutableTenantLimits()->SetDisabledNodesRatioLimit(0);
    cmsConfig.MutableClusterLimits()->SetDisabledNodesRatioLimit(0);
    cmsConfig.MutableSentinelConfig()->SetEnable(options.EnableSentinel);
    SetCmsConfig(cmsConfig);

    // Need to allow restart state storage nodes
    AdvanceCurrentTime(TDuration::Minutes(2));
}


TCmsTestEnv::TCmsTestEnv(ui32 nodeCount,
                         ui32 vdisks,
                         const TNodeTenantsMap &tenants)
    : TCmsTestEnv(TTestEnvOpts(nodeCount, vdisks, tenants))
{
}

TCmsTestEnv::TCmsTestEnv(ui32 nodeCount,
                         const TNodeTenantsMap &tenants)
    : TCmsTestEnv(TTestEnvOpts(nodeCount, 0, tenants))
{
}

TIntrusiveConstPtr<NKikimr::TStateStorageInfo> TCmsTestEnv::GetStateStorageInfo() {
    const TActorId proxy = MakeStateStorageProxyID();
    Send(new IEventHandle(proxy, Sender, new TEvStateStorage::TEvListStateStorage()));

    auto reply = GrabEdgeEventRethrow<TEvStateStorage::TEvListStateStorageResult>(Sender);
    const auto &rec = reply->Get()->Info;
    return rec;
}

void TCmsTestEnv::SetupLogging()
{
    NActors::NLog::EPriority priority = ENABLE_DETAILED_CMS_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;

    SetLogPriority(NKikimrServices::CMS, priority);
}

NCms::TPDiskID TCmsTestEnv::PDiskId(ui32 nodeIndex, ui32 pdiskIndex)
{
    auto pdisks = TFakeNodeWhiteboardService::Info.begin()->second.PDiskStateInfo.size();
    return NCms::TPDiskID(GetNodeId(nodeIndex), GetNodeId(nodeIndex) * pdisks + pdiskIndex);
}

TString TCmsTestEnv::PDiskName(ui32 nodeIndex, ui32 pdiskIndex)
{
    auto id = PDiskId(nodeIndex, pdiskIndex);
    return Sprintf("pdisk-%" PRIu32 "-%" PRIu32, id.NodeId, id.DiskId);
}

void TCmsTestEnv::RestartCms()
{
    Register(CreateTabletKiller(CmsId));

    TDispatchOptions options;
    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
    DispatchEvents(options);
}

void TCmsTestEnv::SendRestartCms()
{
    Register(CreateTabletKiller(CmsId));
}

void TCmsTestEnv::SendToCms(IEventBase *event)
{
    SendToPipe(CmsId, Sender, event, 0, GetPipeConfigWithRetries(), ClientId);
}

void TCmsTestEnv::CreateDefaultCmsPipe()
{
    ClientId = ConnectToPipe(CmsId, Sender, 0, GetPipeConfigWithRetries());
}

void TCmsTestEnv::DestroyDefaultCmsPipe()
{
    ClosePipe(ClientId, Sender, 0);
}

NKikimrCms::TCmsConfig TCmsTestEnv::GetCmsConfig()
{
    auto *request = new TEvCms::TEvGetConfigRequest;
    SendToPipe(CmsId, Sender, request, 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvGetConfigResponse>(handle);
    UNIT_ASSERT(reply);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), TStatus::OK);

    return reply->Record.GetConfig();
}

void TCmsTestEnv::SendCmsConfig(const NKikimrCms::TCmsConfig &config)
{
    auto *request = new TEvCms::TEvSetConfigRequest;
    request->Record.MutableConfig()->CopyFrom(config);
    SendToPipe(CmsId, Sender, request, 0, GetPipeConfigWithRetries());
}

void TCmsTestEnv::SetCmsConfig(const NKikimrCms::TCmsConfig &config)
{
    SendCmsConfig(config);

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvSetConfigResponse>(handle);
    UNIT_ASSERT(reply);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), TStatus::OK);
}

void TCmsTestEnv::SetLimits(ui32 tenantLimit,
                            ui32 tenantRatioLimit,
                            ui32 clusterLimit,
                            ui32 clusterRatioLimit)
{
    NKikimrCms::TCmsConfig config = GetCmsConfig();
    config.MutableTenantLimits()->SetDisabledNodesLimit(tenantLimit);
    config.MutableTenantLimits()->SetDisabledNodesRatioLimit(tenantRatioLimit);
    config.MutableClusterLimits()->SetDisabledNodesLimit(clusterLimit);
    config.MutableClusterLimits()->SetDisabledNodesRatioLimit(clusterRatioLimit);
    SetCmsConfig(config);
}

void TCmsTestEnv::EnableSysNodeChecking() {
    GetAppData().DisableCheckingSysNodesCms = false;
}

NKikimrCms::TClusterState
TCmsTestEnv::RequestState(const NKikimrCms::TClusterStateRequest &request,
                          NKikimrCms::TStatus::ECode code)
{
    TAutoPtr<TEvCms::TEvClusterStateRequest> event = new TEvCms::TEvClusterStateRequest;
    event->Record.CopyFrom(request);
    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvClusterStateResponse>(handle);
    UNIT_ASSERT(reply);

    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);

    return rec.GetState();
}

std::pair<TString, TVector<TString>>
TCmsTestEnv::ExtractPermissions(const NKikimrCms::TPermissionResponse &response)
{
    TVector<TString> ids;
    for (auto &perm : response.GetPermissions())
        ids.push_back(perm.GetId());

    return std::make_pair(response.GetRequestId(), ids);
}

NKikimrCms::TPermissionResponse
TCmsTestEnv::CheckPermissionRequest(TAutoPtr<NCms::TEvCms::TEvPermissionRequest> req,
                                    NKikimrCms::TStatus::ECode code)
{
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvPermissionResponse>(handle);
    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    return rec;
}

NKikimrCms::TManagePermissionResponse
TCmsTestEnv::CheckManagePermissionRequest(const TString &user,
                                          NKikimrCms::TManagePermissionRequest::ECommand cmd,
                                          bool dry,
                                          NKikimrCms::TStatus::ECode code)
{
    auto req = MakeManagePermissionRequest(user, cmd, dry);
    return CheckManagePermissionRequest(req, code);
}

NKikimrCms::TManagePermissionResponse
TCmsTestEnv::CheckManagePermissionRequest(TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest> req,
                                          NKikimrCms::TStatus::ECode code)
{
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvManagePermissionResponse>(handle);
    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    return rec;
}

NKikimrCms::TManageRequestResponse
TCmsTestEnv::CheckManageRequestRequest(TAutoPtr<NCms::TEvCms::TEvManageRequestRequest> req,
                                       NKikimrCms::TStatus::ECode code)
{
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvManageRequestResponse>(handle);
    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    return rec;
}

NKikimrCms::TManagePermissionResponse
TCmsTestEnv::CheckListPermissions(const TString &user, ui64 count)
{
    auto rec = CheckManagePermissionRequest(user, TManagePermissionRequest::LIST);
    UNIT_ASSERT_VALUES_EQUAL(rec.PermissionsSize(), count);
    return rec;
}

void TCmsTestEnv::CheckDonePermission(const TString &user,
                                      const TString &id,
                                      bool dry,
                                      NKikimrCms::TStatus::ECode code)
{
    CheckManagePermissionRequest(user, TManagePermissionRequest::DONE, dry, code, id);
}

void TCmsTestEnv::CheckRejectPermission(const TString &user,
                                        const TString &id,
                                        bool dry,
                                        NKikimrCms::TStatus::ECode code)
{
    CheckManagePermissionRequest(user, TManagePermissionRequest::REJECT, dry, code, id);
}

NKikimrCms::TManagePermissionResponse
TCmsTestEnv::CheckGetPermission(const TString &user,
                                const TString &id,
                                bool dry,
                                NKikimrCms::TStatus::ECode code)
{
    return CheckManagePermissionRequest(user, TManagePermissionRequest::GET, dry, code, id);
}

NKikimrCms::TManageRequestResponse
TCmsTestEnv::CheckGetRequest(const TString &user,
                             const TString &id,
                             bool dry,
                             NKikimrCms::TStatus::ECode code)
{
    auto req = MakeManageRequestRequest(user, TManageRequestRequest::GET, id, dry);
    return CheckManageRequestRequest(req, code);
}

void TCmsTestEnv::CheckRejectRequest(const TString &user,
                                     const TString &id,
                                     bool dry,
                                     NKikimrCms::TStatus::ECode code)
{
    auto req = MakeManageRequestRequest(user, TManageRequestRequest::REJECT, id, dry);
    CheckManageRequestRequest(req, code);
}

NKikimrCms::TManageRequestResponse
TCmsTestEnv::CheckListRequests(const TString &user,
                               ui64 count)
{
    auto req = MakeManageRequestRequest(user, TManageRequestRequest::LIST, false);
    auto rec = CheckManageRequestRequest(req, TStatus::OK);
    UNIT_ASSERT_VALUES_EQUAL(rec.RequestsSize(), count);
    return rec;
}

NKikimrCms::TPermissionResponse
TCmsTestEnv::CheckRequest(const TString &user,
                          TString id,
                          bool dry,
                          NKikimrCms::EAvailabilityMode availabilityMode,
                          TStatus::ECode res,
                          size_t count)
{
    auto request = MakeCheckRequest(user, id, dry, availabilityMode);
    SendToPipe(CmsId, Sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvPermissionResponse>(handle);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), res);
    UNIT_ASSERT_VALUES_EQUAL(rec.PermissionsSize(), count);

    return rec;
}


void TCmsTestEnv::CheckWalleStoreTaskIsFailed(NCms::TEvCms::TEvStoreWalleTask* req)
{
    TString TaskId = req->Task.TaskId;
    SendToPipe(CmsId, Sender, req, 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvStoreWalleTaskFailed>(handle, TDuration::Seconds(30));
    UNIT_ASSERT(reply);
    UNIT_ASSERT_VALUES_EQUAL(reply->TaskId, TaskId);
}

void TCmsTestEnv::CheckWalleCreateTask(TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> req,
                                       NKikimrCms::TStatus::ECode code)
{
    auto id = req->Record.GetTaskId();
    TSet<TString> hosts;
    for (auto &host : req->Record.GetHosts())
        hosts.insert(host);
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvWalleCreateTaskResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    if (rec.GetStatus().GetCode() == TStatus::ERROR_TEMP && rec.GetStatus().GetReason() == "Timeout")
        return CheckWalleCreateTask(req, code);

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetTaskId(), id);
    UNIT_ASSERT_VALUES_EQUAL(rec.HostsSize(), hosts.size());
    for (auto &host : rec.GetHosts())
        UNIT_ASSERT(hosts.contains(host));
}

void TCmsTestEnv::CheckTasksEqual(const NKikimrCms::TWalleTaskInfo &l,
                                  const NKikimrCms::TWalleTaskInfo &r)
{
    UNIT_ASSERT_VALUES_EQUAL(l.GetTaskId(), r.GetTaskId());
    UNIT_ASSERT_VALUES_EQUAL(l.GetStatus(), r.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(l.HostsSize(), r.HostsSize());
    TSet<TString> lh;
    TSet<TString> rh;
    for (auto &host : l.GetHosts())
        lh.insert(host);
    for (auto &host : r.GetHosts())
        rh.insert(host);
    UNIT_ASSERT_VALUES_EQUAL(lh, rh);
}

void TCmsTestEnv::CheckWalleListTasks(const NKikimrCms::TWalleTaskInfo &task)
{
    TAutoPtr<TEvCms::TEvWalleListTasksRequest> event
        = new TEvCms::TEvWalleListTasksRequest;
    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvWalleListTasksResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.TasksSize(), 1);
    CheckTasksEqual(rec.GetTasks(0), task);
}

ui64 TCmsTestEnv::CountWalleTasks()
{
    TAutoPtr<TEvCms::TEvWalleListTasksRequest> event
        = new TEvCms::TEvWalleListTasksRequest;
    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvWalleListTasksResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    return rec.TasksSize();
}

void TCmsTestEnv::CheckWalleListTasks(size_t count)
{
    UNIT_ASSERT_VALUES_EQUAL(CountWalleTasks(), count);
}

void TCmsTestEnv::CheckWalleCheckTask(const TString &id,
                                      TStatus::ECode code,
                                      NKikimrCms::TWalleTaskInfo task)
{
    TAutoPtr<TEvCms::TEvWalleCheckTaskRequest> event
        = new TEvCms::TEvWalleCheckTaskRequest;
    event->Record.SetTaskId(id);
    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvWalleCheckTaskResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    task.SetStatus(rec.GetTask().GetStatus());
    CheckTasksEqual(rec.GetTask(), task);
}

void TCmsTestEnv::CheckWalleCheckTask(const TString &id,
                                      TStatus::ECode code)
{
    TAutoPtr<TEvCms::TEvWalleCheckTaskRequest> event
        = new TEvCms::TEvWalleCheckTaskRequest;
    event->Record.SetTaskId(id);
    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvWalleCheckTaskResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void TCmsTestEnv::CheckWalleRemoveTask(const TString &id,
                                       TStatus::ECode code)
{
    TAutoPtr<TEvCms::TEvWalleRemoveTaskRequest> event
        = new TEvCms::TEvWalleRemoveTaskRequest;
    event->Record.SetTaskId(id);
    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvWalleRemoveTaskResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

TString TCmsTestEnv::CheckNotification(TAutoPtr<TEvCms::TEvNotification> req,
                                       TStatus::ECode code)
{
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvNotificationResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);

    return rec.GetNotificationId();
}

void TCmsTestEnv::CheckSetMarker(TAutoPtr<NCms::TEvCms::TEvSetMarkerRequest> req,
                                 NKikimrCms::TStatus::ECode code)
{
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvSetMarkerResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void TCmsTestEnv::CheckResetMarker(TAutoPtr<NCms::TEvCms::TEvResetMarkerRequest> req,
                                   NKikimrCms::TStatus::ECode code)
{
    SendToPipe(CmsId, Sender, req.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvResetMarkerResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void TCmsTestEnv::CheckGetNotification(const TString &user,
                                       const TString &id,
                                       TStatus::ECode code)
{
    TAutoPtr<TEvCms::TEvManageNotificationRequest> event
        = new TEvCms::TEvManageNotificationRequest;
    event->Record.SetCommand(TManageNotificationRequest::GET);
    if (user)
        event->Record.SetUser(user);
    if (id)
        event->Record.SetNotificationId(id);

    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvManageNotificationResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void TCmsTestEnv::CheckListNotifications(const TString &user,
                                         TStatus::ECode code,
                                         ui32 count)
{
    TAutoPtr<TEvCms::TEvManageNotificationRequest> event
        = new TEvCms::TEvManageNotificationRequest;
    event->Record.SetCommand(TManageNotificationRequest::LIST);
    if (user)
        event->Record.SetUser(user);

    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvManageNotificationResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    UNIT_ASSERT_VALUES_EQUAL(rec.NotificationsSize(), count);
}

void TCmsTestEnv::CheckRejectNotification(const TString &user,
                                          const TString &id,
                                          TStatus::ECode code,
                                          bool dry)
{
    TAutoPtr<TEvCms::TEvManageNotificationRequest> event
        = new TEvCms::TEvManageNotificationRequest;
    event->Record.SetCommand(TManageNotificationRequest::REJECT);
    if (user)
        event->Record.SetUser(user);
    if (id)
        event->Record.SetNotificationId(id);
    if (dry)
        event->Record.SetDryRun(dry);

    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvManageNotificationResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void TCmsTestEnv::WaitUpdatDiskStatus(ui32 statusEventsCount,
                                        NKikimrBlobStorage::EDriveStatus newStatus)
{
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TIsUpdateStatusConfigRequest(newStatus), statusEventsCount);
    UNIT_ASSERT(DispatchEvents(options));
}

void TCmsTestEnv::EnableBSBaseConfig()
{
    TFakeNodeWhiteboardService::Config.MutableResponse()->SetSuccess(true);
    if (TFakeNodeWhiteboardService::Config.MutableResponse()->StatusSize())
        TFakeNodeWhiteboardService::Config.MutableResponse()->MutableStatus(0)->SetSuccess(true);
}

void TCmsTestEnv::DisableBSBaseConfig()
{
    TFakeNodeWhiteboardService::Config.MutableResponse()->SetSuccess(false);
    if (TFakeNodeWhiteboardService::Config.MutableResponse()->StatusSize())
        TFakeNodeWhiteboardService::Config.MutableResponse()->MutableStatus(0)->SetSuccess(false);
}

NKikimrCms::TGetLogTailResponse TCmsTestEnv::GetLogTail(ui32 type,
                                                        TInstant from,
                                                        TInstant to,
                                                        ui32 limit,
                                                        ui32 offset)
{
    TAutoPtr<TEvCms::TEvGetLogTailRequest> event
        = new TEvCms::TEvGetLogTailRequest;
    event->Record.SetIncludeData(true);
    auto &filter = *event->Record.MutableLogFilter();
    filter.SetRecordType(type);
    filter.SetMinTimestamp(from.GetValue());
    filter.SetMaxTimestamp(to.GetValue());
    filter.SetLimit(limit);
    filter.SetOffset(offset);

    SendToPipe(CmsId, Sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = GrabEdgeEventRethrow<TEvCms::TEvGetLogTailResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), TStatus::OK);

    return rec;
}

void TCmsTestEnv::AddBSCFailures(const NCms::TPDiskID& id, TVector<bool> &&failuresPattern) {
    TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
    auto& vec = TFakeNodeWhiteboardService::BSControllerResponsePatterns[id];
    std::move(failuresPattern.begin(), failuresPattern.end(), std::back_inserter(vec));
}

void TCmsTestEnv::EnableNoisyBSCPipe() {
    TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
    TFakeNodeWhiteboardService::NoisyBSCPipe = true;
}

void TCmsTestEnv::RegenerateBSConfig(NKikimrBlobStorage::TBaseConfig *config, const TTestEnvOpts &opts) {
    TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
    config->Clear();
    GenerateExtendedInfo(*this, config, opts.VDisks, 4, opts.Tenants, opts.UseMirror3dcErasure);
}

} // namespace NCmsTest
} // namespace NKikimr
