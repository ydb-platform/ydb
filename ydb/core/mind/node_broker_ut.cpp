#include "node_broker_impl.h"
#include "dynamic_nameserver_impl.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/fake_coordinator.h>

#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/system/hostname.h>

const bool STRAND_PDISK = true;
#ifndef NDEBUG
const bool ENABLE_DETAILED_NODE_BROKER_LOG = true;
#else
const bool ENABLE_DETAILED_NODE_BROKER_LOG = false;
#endif

namespace NKikimr {

using namespace NNodeBroker;
using namespace NKikimrNodeBroker;

namespace {

const TString DOMAIN_NAME = "dc-1";

void SetupLogging(TTestActorRuntime& runtime)
{
    NActors::NLog::EPriority priority = ENABLE_DETAILED_NODE_BROKER_LOG ? NLog::PRI_TRACE : NLog::PRI_ERROR;

    runtime.SetLogPriority(NKikimrServices::NODE_BROKER, priority);
}

THashMap<ui32, TIntrusivePtr<TNodeWardenConfig>> NodeWardenConfigs;

void SetupServices(TTestActorRuntime &runtime,
                   ui32 maxDynNodes)
{
    const ui32 domainsNum = 1;
    const ui32 disksInDomain = 1;

    // setup domain info
    TAppPrepare app;

    app.ClearDomainsAndHive();
    ui32 domainUid = TTestTxConfig::DomainUid;
    ui32 planResolution = 50;
    ui64 schemeRoot = TTestTxConfig::SchemeShard;
    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
        DOMAIN_NAME, domainUid, schemeRoot, planResolution,
        TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
        TVector<ui64>{},
        TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
        DefaultPoolKinds(2));

    TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
    ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
    runtime.SetTxAllocatorTabletIds(ids);
    app.AddDomain(domain.Release());

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
        SetupStateStorage(runtime, nodeIndex);

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

        TIntrusivePtr<TNodeWardenConfig> existingNodeWardenConfig = NodeWardenConfigs[nodeIndex];
        if (existingNodeWardenConfig != nullptr) {
            //std::swap(nodeWardenConfig->SectorMaps, existingNodeWardenConfig->SectorMaps);
        }

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

        NodeWardenConfigs[nodeIndex] = nodeWardenConfig;

        SetupBSNodeWarden(runtime, nodeIndex, nodeWardenConfig);
        SetupNodeWhiteboard(runtime, nodeIndex);
        SetupTabletResolver(runtime, nodeIndex);
        SetupResourceBroker(runtime, nodeIndex, {});
        SetupSharedPageCache(runtime, nodeIndex, NFake::TCaches{
            .Shared = 1,
        });
        SetupSchemeCache(runtime, nodeIndex, DOMAIN_NAME);
    }

    runtime.Initialize(app.Unwrap());

    runtime.GetAppData().DynamicNameserviceConfig = new TDynamicNameserviceConfig;
    auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
    dnConfig->MaxStaticNodeId = 1023;
    dnConfig->MinDynamicNodeId = 1024;
    dnConfig->MaxDynamicNodeId = 1024 + (maxDynNodes - 1);
    runtime.GetAppData().FeatureFlags.SetEnableNodeBrokerSingleDomainMode(true);
    runtime.GetAppData().FeatureFlags.SetEnableStableNodeNames(true);
     
    if (!runtime.IsRealThreads()) {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvLocalRecoveryDone,
                                                                             domainsNum * disksInDomain));
        runtime.DispatchEvents(options);
    }

    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
    BootFakeCoordinator(runtime, TTestTxConfig::Coordinator, MakeIntrusive<TFakeCoordinator::TState>());
    auto aid = CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeNodeBrokerID(), TTabletTypes::NodeBroker), &CreateNodeBroker);
    runtime.EnableScheduleForActor(aid, true);
}

void SetConfig(TTestActorRuntime& runtime,
               TActorId sender,
               const NKikimrNodeBroker::TConfig &config)
{
    auto event = MakeHolder<TEvNodeBroker::TEvSetConfigRequest>();
    event->Record.MutableConfig()->CopyFrom(config);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodeBroker::TEvSetConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), TStatus::OK);
}

void SetEpochDuration(TTestActorRuntime& runtime,
                      TActorId sender,
                      TDuration lease)
{
    NKikimrNodeBroker::TConfig config;
    config.SetEpochDuration(lease.GetValue());
    SetConfig(runtime, sender, config);
}

void SetBannedIds(TTestActorRuntime& runtime,
                  TActorId sender,
                  const TVector<std::pair<ui32, ui32>> ids)
{
    NKikimrNodeBroker::TConfig config;
    for (auto &pr : ids) {
        auto &entry = *config.AddBannedNodeIds();
        entry.SetFrom(pr.first);
        entry.SetTo(pr.second);
    }
    SetConfig(runtime, sender, config);
}

void Setup(TTestActorRuntime& runtime,
           ui32 maxDynNodes = 3,
           const TVector<TString>& databases = {})
{
    using namespace NMalloc;
    TMallocInfo mallocInfo = MallocInfo();
    mallocInfo.SetParam("FillMemoryOnAllocation", "false");

    auto scheduledFilter = [](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
        if (event->HasEvent()
            && event->Type == TDynamicNameserver::TEvPrivate::TEvUpdateEpoch::EventType)
            return false;
        return TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
    };
    runtime.SetScheduledEventFilter(scheduledFilter);

    SetupLogging(runtime);
    SetupServices(runtime, maxDynNodes);

    TActorId sender = runtime.AllocateEdgeActor();
    ui32 txId = 100;
    for (const auto& database : databases) {
        auto splittedPath = SplitPath(database);
        const auto databaseName = splittedPath.back();
        splittedPath.pop_back();
        do {
            auto modifyScheme = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            modifyScheme->Record.SetTxId(++txId);
            auto* transaction = modifyScheme->Record.AddTransaction();
            transaction->SetWorkingDir(CanonizePath(splittedPath));
            transaction->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExtSubDomain);
            auto* subdomain = transaction->MutableSubDomain();
            subdomain->SetName(databaseName);
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, modifyScheme.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);
    }
}

bool IsTabletActiveEvent(IEventHandle& ev)
{
    if (ev.GetTypeRewrite() == NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate) {
        if (ev.Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>()->Record.GetState()
            == NKikimrWhiteboard::TTabletStateInfo::Active) {
            return true;
        }
    }
    return false;
}

TAutoPtr<TEvNodeBroker::TEvRegistrationRequest>
MakeRegistrationRequest(const TString &host,
                        ui16 port,
                        const TString &resolveHost,
                        const TString &address,
                        const TString &path = DOMAIN_NAME,
                        ui64 dc = 0,
                        ui64 room = 0,
                        ui64 rack = 0,
                        ui64 body = 0,
                        bool fixed = false)
{
    TAutoPtr<TEvNodeBroker::TEvRegistrationRequest> event = new TEvNodeBroker::TEvRegistrationRequest;
    event->Record.SetHost(host);
    event->Record.SetPort(port);
    event->Record.SetResolveHost(resolveHost);
    event->Record.SetAddress(address);
    auto &loc = *event->Record.MutableLocation();
    loc.SetDataCenter(ToString(dc));
    loc.SetModule(ToString(room));
    loc.SetRack(ToString(rack));
    loc.SetUnit(ToString(body));
    event->Record.SetFixedNodeId(fixed);
    event->Record.SetPath(path);
    return event;
}

void CheckRegistration(TTestActorRuntime &runtime,
                       TActorId sender,
                       const TString &host,
                       ui16 port,
                       const TString &resolveHost,
                       const TString &address,
                       ui64 dc = 0,
                       ui64 room = 0,
                       ui64 rack = 0,
                       ui64 body = 0,
                       TStatus::ECode code = TStatus::OK,
                       ui32 nodeId = 0,
                       ui64 expire = 0,
                       bool fixed = false,
                       const TString &path = DOMAIN_NAME,
                       const TMaybe<TKikimrScopeId> &scopeId = {},
                       const TString &name = "")
{
    auto event = MakeRegistrationRequest(host, port, resolveHost, address, path, dc, room, rack, body, fixed);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvRegistrationResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);

    if (code == TStatus::OK) {
        if (nodeId)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetNodeId(), nodeId);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetHost(), host);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetPort(), port);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetResolveHost(), resolveHost);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetAddress(), address);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetDataCenter(), ToString(dc));
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetModule(), ToString(room));
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetRack(), ToString(rack));
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetUnit(), ToString(body));
        if (expire)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetExpire(), expire);
        if (scopeId) {
            UNIT_ASSERT_VALUES_EQUAL(rec.GetScopeTabletId(), scopeId->GetSchemeshardId());
            UNIT_ASSERT_VALUES_EQUAL(rec.GetScopePathId(), scopeId->GetPathItemId());
        }
        if (name) {
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetName(), name);
        }
    }
}

void CheckRegistration(TTestActorRuntime &runtime,
                       TActorId sender,
                       const TString &host,
                       ui16 port,
                       const TString &path,
                       TStatus::ECode code = TStatus::OK,
                       ui32 nodeId = 0,
                       ui64 expire = 0,
                       const TString &name = "")
{
    CheckRegistration(runtime, sender, host, port, host, "", 0, 0, 0, 0, code, nodeId, expire,
                      false, path, Nothing(), name);
}

NKikimrNodeBroker::TEpoch GetEpoch(TTestActorRuntime &runtime,
                                   TActorId sender)
{
    TAutoPtr<TEvNodeBroker::TEvListNodes> event = new TEvNodeBroker::TEvListNodes;
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodesInfo>(handle);
    return reply->GetRecord().GetEpoch();
}

NKikimrNodeBroker::TEpoch WaitForEpochUpdate(TTestActorRuntime &runtime,
                                             TActorId sender)
{
    auto epoch = GetEpoch(runtime, sender);

    if (runtime.GetCurrentTime() < TInstant::FromValue(epoch.GetEnd())) {
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));

        struct TIsEvUpdateEpoch {
            bool operator()(IEventHandle &ev) {
                if (ev.HasEvent()
                    && ev.Type == TNodeBroker::TEvPrivate::TEvUpdateEpoch::EventType)
                    return true;
                return false;
            }
        };

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TIsEvUpdateEpoch(), 1);
        runtime.DispatchEvents(options);
    }

    ui64 reqEpoch = epoch.GetId() + 1;
    while (epoch.GetId() != reqEpoch) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NKikimr::TEvents::TEvFlushLog::EventType, 1);
        runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        epoch = GetEpoch(runtime, sender);
    }

    return epoch;
}

void CheckNodesListResponse(const NKikimrNodeBroker::TNodesInfo &rec,
                            TSet<ui64> ids,
                            TSet<ui64> expiredIds)
{
    UNIT_ASSERT_VALUES_EQUAL(rec.NodesSize(), ids.size());
    for (auto &node : rec.GetNodes()) {
        UNIT_ASSERT(ids.contains(node.GetNodeId()));
        ids.erase(node.GetNodeId());
    }
    UNIT_ASSERT_VALUES_EQUAL(rec.ExpiredNodesSize(), expiredIds.size());
    for (auto &node : rec.GetExpiredNodes()) {
        UNIT_ASSERT(expiredIds.contains(node.GetNodeId()));
        expiredIds.erase(node.GetNodeId());
    }
}

NKikimrNodeBroker::TEpoch CheckFilteredNodesList(TTestActorRuntime &runtime,
                                                 TActorId sender,
                                                 TSet<ui64> ids,
                                                 TSet<ui64> expiredIds,
                                                 ui64 minEpoch,
                                                 ui64 cachedVersion = 0)
{
    UNIT_ASSERT(!minEpoch || !cachedVersion);
    UNIT_ASSERT(minEpoch || cachedVersion);

    auto epoch = GetEpoch(runtime, sender);

    TAutoPtr<IEventHandle> handle;
    TAutoPtr<TEvNodeBroker::TEvListNodes> event = new TEvNodeBroker::TEvListNodes;
    if (minEpoch)
        event->Record.SetMinEpoch(minEpoch);
    if (cachedVersion)
        event->Record.SetCachedVersion(cachedVersion);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    if (minEpoch) {
        while (minEpoch > epoch.GetId()) {
            auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodesInfo>(handle, TDuration::Seconds(1));
            UNIT_ASSERT(!reply);

            if (minEpoch > epoch.GetId() + 1) {
                WaitForEpochUpdate(runtime, sender);
                epoch = GetEpoch(runtime, sender);
            } else {
                runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));
                break;
            }
        }
    }

    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodesInfo>(handle, TDuration::Seconds(1));
    UNIT_ASSERT(reply);
    const auto &rec = reply->GetRecord();
    CheckNodesListResponse(rec, ids, expiredIds);

    return rec.GetEpoch();
}

NKikimrNodeBroker::TEpoch CheckNodesList(TTestActorRuntime &runtime,
                                         TActorId sender,
                                         TSet<ui64> ids,
                                         TSet<ui64> expiredIds,
                                         ui64 epoch)
{
    TAutoPtr<TEvNodeBroker::TEvListNodes> event = new TEvNodeBroker::TEvListNodes;
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodesInfo>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->GetRecord();
    CheckNodesListResponse(rec, ids, expiredIds);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetId(), epoch);

    return rec.GetEpoch();
}

void CheckNodeInfo(TTestActorRuntime &runtime,
                   TActorId sender,
                   ui32 nodeId,
                   TStatus::ECode code)
{
    TAutoPtr<TEvNodeBroker::TEvResolveNode> event = new TEvNodeBroker::TEvResolveNode;
    event->Record.SetNodeId(nodeId);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvResolvedNode>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void CheckNodeInfo(TTestActorRuntime &runtime,
                   TActorId sender,
                   ui32 nodeId,
                   const TString &host,
                   ui16 port,
                   const TString &resolveHost,
                   const TString &address,
                   ui64 dc = 0,
                   ui64 room = 0,
                   ui64 rack = 0,
                   ui64 body = 0,
                   ui64 expire = 0)
{
    TAutoPtr<TEvNodeBroker::TEvResolveNode> event = new TEvNodeBroker::TEvResolveNode;
    event->Record.SetNodeId(nodeId);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvResolvedNode>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), TStatus::OK);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetNodeId(), nodeId);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetHost(), host);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetPort(), port);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetResolveHost(), resolveHost);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetAddress(), address);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetDataCenter(), ToString(dc));
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetModule(), ToString(room));
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetRack(), ToString(rack));
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetUnit(), ToString(body));
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetExpire(), expire);
}

void CheckLeaseExtension(TTestActorRuntime &runtime,
                         TActorId sender,
                         ui32 nodeId,
                         TStatus::ECode code,
                         const NKikimrNodeBroker::TEpoch &epoch = {},
                         bool fixed = false)
{
    TAutoPtr<TEvNodeBroker::TEvExtendLeaseRequest> event = new TEvNodeBroker::TEvExtendLeaseRequest;
    event->Record.SetNodeId(nodeId);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvExtendLeaseResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    if (code == TStatus::OK) {
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNodeId(), nodeId);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().DebugString(), epoch.DebugString());
        if (fixed)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetExpire(), Max<ui64>());
        else
            UNIT_ASSERT_VALUES_EQUAL(rec.GetExpire(), epoch.GetNextEnd());
    }
}

void CheckResolveNode(TTestActorRuntime &runtime,
                      TActorId sender,
                      ui32 nodeId,
                      const TString &addr)
{
    TAutoPtr<TEvInterconnect::TEvResolveNode> event = new TEvInterconnect::TEvResolveNode(nodeId);
    runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, event.Release()));

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvLocalNodeInfo>(handle);
    UNIT_ASSERT(reply);

    UNIT_ASSERT_VALUES_EQUAL(reply->NodeId, nodeId);
    UNIT_ASSERT_VALUES_EQUAL(reply->Addresses[0].GetAddress(), addr);
}

void CheckResolveUnknownNode(TTestActorRuntime &runtime,
                             TActorId sender,
                             ui32 nodeId)
{
    TAutoPtr<TEvInterconnect::TEvResolveNode> event = new TEvInterconnect::TEvResolveNode(nodeId);
    runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, event.Release()));

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvLocalNodeInfo>(handle);
    UNIT_ASSERT(reply);

    UNIT_ASSERT_VALUES_EQUAL(reply->NodeId, nodeId);
    UNIT_ASSERT(reply->Addresses.empty());
}

void GetNameserverNodesList(TTestActorRuntime &runtime,
                            TActorId sender,
                            THashMap<ui32, TEvInterconnect::TNodeInfo> &nodes,
                            bool includeStatic)
{
    ui32 maxStaticNodeId = runtime.GetAppData().DynamicNameserviceConfig->MaxStaticNodeId;
    TAutoPtr<TEvInterconnect::TEvListNodes> event = new TEvInterconnect::TEvListNodes;
    runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, event.Release()));

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handle);
    UNIT_ASSERT(reply);

    for (auto &node : reply->Nodes)
        if (includeStatic || node.NodeId > maxStaticNodeId)
            nodes.emplace(node.NodeId, node);
}

void CheckNameserverNodesList(TTestActorRuntime &runtime,
                              TActorId sender,
                              size_t count)
{
    THashMap<ui32, TEvInterconnect::TNodeInfo> nodes;
    GetNameserverNodesList(runtime, sender, nodes, true);
    UNIT_ASSERT_VALUES_EQUAL(nodes.size(), count);
}

TEvInterconnect::TNodeInfo MakeICNodeInfo(ui32 nodeId,
                                          const TString &host,
                                          ui16 port,
                                          const TString &resolveHost,
                                          const TString &address,
                                          ui64 dc,
                                          ui64 room,
                                          ui64 rack,
                                          ui64 body)
{
    NActorsInterconnect::TNodeLocation location;
    location.SetDataCenter(ToString(dc));
    location.SetModule(ToString(room));
    location.SetRack(ToString(rack));
    location.SetUnit(ToString(body));
    return TEvInterconnect::TNodeInfo(nodeId, address, host, resolveHost, port, TNodeLocation(location));
}

void CheckNameserverDynamicNodesList(const THashMap<ui32, TEvInterconnect::TNodeInfo> &nodes,
                                     const TEvInterconnect::TNodeInfo &node)
{
    auto it = nodes.find(node.NodeId);
    UNIT_ASSERT(it != nodes.end());
    UNIT_ASSERT_VALUES_EQUAL(it->second.Address, node.Address);
    UNIT_ASSERT_VALUES_EQUAL(it->second.Host, node.Host);
    UNIT_ASSERT_VALUES_EQUAL(it->second.ResolveHost, node.ResolveHost);
    UNIT_ASSERT_VALUES_EQUAL(it->second.Port, node.Port);
    UNIT_ASSERT_EQUAL(it->second.Location, node.Location);
}

template<typename... Ts>
void CheckNameserverDynamicNodesList(const THashMap<ui32, TEvInterconnect::TNodeInfo> &nodes,
                                     const TEvInterconnect::TNodeInfo &node,
                                     Ts... args)
{
    CheckNameserverDynamicNodesList(nodes, node);
    CheckNameserverDynamicNodesList(nodes, args...);
}

template<typename... Ts>
void CheckNameserverDynamicNodesList(TTestActorRuntime &runtime,
                                     TActorId sender,
                                     size_t count,
                                     Ts... args)
{
    THashMap<ui32, TEvInterconnect::TNodeInfo> nodes;
    GetNameserverNodesList(runtime, sender, nodes, false);
    UNIT_ASSERT_VALUES_EQUAL(nodes.size(), count);
    CheckNameserverDynamicNodesList(nodes, args...);
}

void CheckGetNode(TTestActorRuntime &runtime,
                  TActorId sender,
                  ui32 nodeId,
                  bool exists)
{
    TAutoPtr<TEvInterconnect::TEvGetNode> event = new TEvInterconnect::TEvGetNode(nodeId);
    runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, event.Release()));

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodeInfo>(handle);
    UNIT_ASSERT(reply);

    UNIT_ASSERT_VALUES_EQUAL((bool)reply->Node, exists);
}

void RestartNodeBroker(TTestActorRuntime &runtime)
{
    TDispatchOptions options;
    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
    runtime.Register(CreateTabletKiller(MakeNodeBrokerID()));
    runtime.DispatchEvents(options);
}

} // anonymous namespace

static constexpr ui32 NODE1 = 1024;
static constexpr ui32 NODE2 = 1025;
static constexpr ui32 NODE3 = 1026;
static constexpr ui32 NODE4 = 1027;
static constexpr ui32 NODE5 = 1028;
static constexpr ui32 NODE6 = 1029;
static constexpr ui32 NODE7 = 1030;
static constexpr ui32 NODE8 = 1031;
static constexpr ui32 NODE9 = 1032;
static constexpr ui32 NODE10 = 1033;

Y_UNIT_TEST_SUITE(TNodeBrokerTest) {
    Y_UNIT_TEST(BasicFunctionality)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);
        // Register node NODE1.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        // Check node NODE1 exists.
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        // Nodes list now has 1 node.
        CheckNodesList(runtime, sender, {NODE1}, {}, 1);
        // Register node NODE2.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2, epoch.GetNextEnd());

        // Wait until epoch expiration.
        WaitForEpochUpdate(runtime, sender);
        epoch = CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, 2);

        // Extend lease for node NODE1.
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        // Kill tablet, wait for node NODE2 expiration and check state is restored correctly.
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));

        RestartNodeBroker(runtime);

        epoch = CheckNodesList(runtime, sender, {NODE1}, {NODE2}, 3);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetEnd());
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);

        // Register node NODE3.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::OK, NODE3, epoch.GetNextEnd());
        // Register node with existing lease, this should extend lease to the next epoch.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        // Registration of existing node with different location.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 5, TStatus::WRONG_REQUEST, NODE1);
        // Registration of existing node with different address, expect new node id.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.14",
                          1, 2, 3, 4, TStatus::OK, NODE4, epoch.GetNextEnd());
        // There should be no more free IDs.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        // Extend lease for expired node.
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        // Extend lease for unknown node.
        CheckLeaseExtension(runtime, sender, 1025, TStatus::WRONG_REQUEST);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE3, NODE4}, {}, 4);

        // Register node and re-use NODE2 node ID.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::OK, NODE2, epoch.GetNextEnd());
        epoch.SetVersion(epoch.GetVersion() + 1);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {NODE3, NODE4}, 5);

        WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1, NODE2}, 6);
    }

    Y_UNIT_TEST(FixedNodeId)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        // Register node NODE1 with fixed ID.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, Max<ui64>(), true);
        epoch.SetVersion(epoch.GetVersion() + 1);
        // Check node NODE1 exists and has infinite lease.
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, Max<ui64>());
        // Lease extension should work fine and report infinite lease.
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch, true);
        // Register node NODE1 without fixed ID option. This shouldn't unfix ID.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, Max<ui64>(), false);
        // Check node still has infinite lease.
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, Max<ui64>());

        // Register node NODE2.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2, epoch.GetNextEnd(), false);
        epoch.SetVersion(epoch.GetVersion() + 1);
        // Now register it again but with fixed ID.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2, Max<ui64>(), true);
        // Check node NODE2 has infinite lease and ping doesn't affect it.
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                      1, 2, 3, 5, Max<ui64>());
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch, true);
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                      1, 2, 3, 5, Max<ui64>());
        // Check tegular registration doesn't affect infinite lease.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2, Max<ui64>(), false);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch, true);
    }

    Y_UNIT_TEST(TestListNodes)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 10);
        TActorId sender = runtime.AllocateEdgeActor();

        WaitForEpochUpdate(runtime, sender);
        WaitForEpochUpdate(runtime, sender);
        auto epoch = GetEpoch(runtime, sender);
        // Register node NODE1.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        epoch = CheckFilteredNodesList(runtime, sender, {NODE1}, {}, epoch.GetId(), 0);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() + 1);
        auto epoch1 = CheckFilteredNodesList(runtime, sender, {NODE1}, {}, epoch.GetId() - 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(epoch1.GetId(), epoch.GetId());
        epoch1 = CheckFilteredNodesList(runtime, sender, {NODE1}, {}, epoch.GetId() + 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(epoch1.GetId(), epoch.GetId() + 1);
        epoch1 = CheckFilteredNodesList(runtime, sender, {}, {NODE1}, epoch.GetId() + 2, 0);
        UNIT_ASSERT_VALUES_EQUAL(epoch1.GetId(), epoch.GetId() + 2);
        epoch1 = CheckFilteredNodesList(runtime, sender, {}, {}, epoch.GetId() + 5, 0);
        UNIT_ASSERT_VALUES_EQUAL(epoch1.GetId(), epoch.GetId() + 5);
    }

    Y_UNIT_TEST(TestRandomActions)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 10);
        TActorId sender = runtime.AllocateEdgeActor();

        TVector<TString> hosts =
            { "host1", "host2", "host3", "host4", "host5",
              "host6", "host7", "host8", "host9", "host10",
              "host11", "host12", "host13", "host14", "host15" };

        enum EAction {
            REGISTER,
            PING,
            RESTART,
            RESOLVE_NODE,
            LIST_NODES,
            MOVE_EPOCH,
        };

        struct TState {
            ui64 NodeId = 0;
            ui64 PingEpoch = 0;
        };
        auto epoch = GetEpoch(runtime, sender);

        THashMap<TString, TState> state;
        TSet<ui64> expired;
        TSet<ui64> freeIds = {
            NODE1, NODE2, NODE3, NODE4, NODE5,
            NODE6, NODE7, NODE8, NODE9, NODE10
        };

        TVector<EAction> actions = {
            REGISTER, REGISTER, REGISTER, REGISTER, REGISTER,
            PING, PING, PING, PING, PING, PING, PING,
            RESTART, RESOLVE_NODE, RESOLVE_NODE, LIST_NODES,
            MOVE_EPOCH
        };

        for (size_t i = 0; i < 3000; ++i) {
            EAction action = actions[RandomNumber<ui64>(actions.size())];
            switch (action) {
            case REGISTER:
                {
                    ui64 no = RandomNumber<ui64>(hosts.size());
                    TString host = hosts[no];
                    TStatus::ECode code = TStatus::OK;
                    ui64 nodeId = 0;
                    if (state.contains(host)) {
                        nodeId = state[host].NodeId;
                        if (state[host].PingEpoch != epoch.GetId()) {
                            //epoch.SetVersion(epoch.GetVersion() + 1);
                            state[host].PingEpoch = epoch.GetId();
                        }
                    } else {
                        if (freeIds.empty())
                            code = TStatus::ERROR_TEMP;
                        else {
                            nodeId = *freeIds.begin();
                            freeIds.erase(nodeId);
                            state[host] = TState{nodeId, epoch.GetId()};
                            epoch.SetVersion(epoch.GetVersion() + 1);
                        }
                    }
                    CheckRegistration(runtime, sender, host, no, host, host,
                                      no, no, no, no, code, nodeId,
                                      epoch.GetNextEnd(), false);
                }
                break;
            case MOVE_EPOCH:
                {
                    epoch.SetVersion(epoch.GetVersion() + 1);
                    epoch.SetId(epoch.GetId() + 1);
                    ui64 expire = epoch.GetNextEnd() * 2 - epoch.GetEnd();
                    epoch.SetStart(epoch.GetEnd());
                    epoch.SetEnd(epoch.GetNextEnd());
                    epoch.SetNextEnd(expire);
                    TVector<TString> toRemove;
                    for (auto id : expired) {
                        freeIds.insert(id);
                    }
                    expired.clear();
                    for (auto it = state.begin(); it != state.end(); ) {
                        auto next = it;
                        ++next;
                        if (it->second.PingEpoch == (epoch.GetId() - 2)) {
                            expired.insert(it->second.NodeId);
                            state.erase(it);
                        }
                        it = next;
                    }
                    WaitForEpochUpdate(runtime, sender);
                    auto e = GetEpoch(runtime, sender);
                    UNIT_ASSERT_VALUES_EQUAL(epoch.DebugString(), e.DebugString());
                }
                break;
            case PING:
                {
                    ui64 no = RandomNumber<ui64>(hosts.size());
                    TString host = hosts[no];
                    TStatus::ECode code = TStatus::OK;
                    ui64 nodeId = 0;

                    if (state.contains(host)) {
                        nodeId = state[host].NodeId;
                        if (state[host].PingEpoch == epoch.GetId()) {
                            // no modifications
                        } else if (state[host].PingEpoch == (epoch.GetId() - 1)) {
                            state[host].PingEpoch = epoch.GetId();
                            //epoch.SetVersion(epoch.GetVersion() + 1);
                        } else {
                            code = TStatus::WRONG_REQUEST;
                        }
                    } else {
                        if (!freeIds.empty())
                            nodeId = *freeIds.begin();
                        else if (!expired.empty())
                            nodeId = *expired.begin();
                        code = TStatus::WRONG_REQUEST;
                    }

                    CheckLeaseExtension(runtime, sender, nodeId, code, epoch);
                }
                break;
            case RESOLVE_NODE:
                {
                    ui64 no = RandomNumber<ui64>(hosts.size());
                    TString host = hosts[no];
                    TStatus::ECode code = TStatus::OK;
                    ui64 nodeId = 0;
                    ui64 expire = 0;

                    if (state.contains(host)) {
                        nodeId = state[host].NodeId;
                        if (state[host].PingEpoch == epoch.GetId())
                            expire = epoch.GetNextEnd();
                        else
                            expire = epoch.GetEnd();
                    } else {
                        if (!freeIds.empty())
                            nodeId = *freeIds.begin();
                        else if (!expired.empty())
                            nodeId = *expired.begin();
                        code = TStatus::WRONG_REQUEST;
                    }

                    if (code != TStatus::OK)
                        CheckNodeInfo(runtime, sender, nodeId, code);
                    else
                        CheckNodeInfo(runtime, sender, nodeId, host, no, host, host,
                                      no, no, no, no, expire);
                }
                break;
            case LIST_NODES:
                {
                    TSet<ui64> nodes;
                    for (auto &pr : state)
                        nodes.insert(pr.second.NodeId);
                    CheckNodesList(runtime, sender, nodes, expired, epoch.GetId());
                }
                break;
            case RESTART:
                {
                    TDispatchOptions options;
                    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
                    runtime.Register(CreateTabletKiller(MakeNodeBrokerID()));
                    runtime.DispatchEvents(options);
                }
                break;
            }
        }
    }

    Y_UNIT_TEST(SingleDomainModeBannedIds) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, 10);
        TActorId sender = runtime.AllocateEdgeActor();

        SetBannedIds(runtime, sender, {{NODE2, NODE9}});

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);

        // Register node NODE1.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        // Register node NODE10.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE10, epoch.GetNextEnd());
        // No more free IDs.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::ERROR_TEMP);

        SetBannedIds(runtime, sender, {{NODE2, NODE6}, {NODE8, NODE9}});
        // Register node NODE7.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::OK, NODE7, epoch.GetNextEnd());
        // No more free IDs.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        epoch = GetEpoch(runtime, sender);

        // Now ban registered node and check lease extension.
        SetBannedIds(runtime, sender, {{NODE1, NODE6}, {NODE8, NODE9}});
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
        CheckLeaseExtension(runtime, sender, NODE7, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE10, TStatus::OK, epoch);

        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Wait until node NODE1 expires.
        WaitForEpochUpdate(runtime, sender);
        epoch = GetEpoch(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE7, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE10, TStatus::OK, epoch);
        WaitForEpochUpdate(runtime, sender);

        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
        // No more free IDs still.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        RestartNodeBroker(runtime);

        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);
    }

    Y_UNIT_TEST(ExtendLeaseRestartRace) {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);
        // Register node NODE1.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());

        // Compact node broker tables to have page faults on reboot
        runtime.SendToPipe(MakeNodeBrokerID(), sender, new TEvNodeBroker::TEvCompactTables(), 0, GetPipeConfigWithRetries());
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Wait until epoch expiration.
        WaitForEpochUpdate(runtime, sender);
        epoch = CheckNodesList(runtime, sender, {NODE1}, {}, 2);

        class THooks : public INodeBrokerHooks {
        public:
            THooks(TTestActorRuntime& runtime)
                : Runtime(runtime)
            { }

            virtual void OnActivateExecutor(ui64 tabletId) override {
                Cerr << "... OnActivateExecutor tabletId# " << tabletId << Endl;
                Y_UNUSED(tabletId);
                Activated = true;
            }

            void Install() {
                if (!Installed) {
                    PrevObserverFunc = Runtime.SetObserverFunc([this](auto& event) {
                        return this->OnEvent(event);
                    });
                    INodeBrokerHooks::Set(this);
                    Installed = true;
                }
            }

            void Uninstall() {
                if (Installed) {
                    INodeBrokerHooks::Set(nullptr);
                    Runtime.SetObserverFunc(PrevObserverFunc);
                    Installed = false;
                }
            }

            bool HasCacheRequests() const {
                return !CacheRequests.empty();
            }

            void WaitCacheRequests() {
                while (!HasCacheRequests()) {
                    Y_ABORT_UNLESS(Installed);
                    TDispatchOptions options;
                    options.CustomFinalCondition = [this]() {
                        return HasCacheRequests();
                    };
                    Runtime.DispatchEvents(options);
                }
            }

            void ResendCacheRequests() {
                for (auto& ev : CacheRequests) {
                    Runtime.Send(ev.Release(), 0, true);
                }
                CacheRequests.clear();
            }

        private:
            TTestActorRuntime::EEventAction OnEvent(TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case NSharedCache::EvRequest: {
                        if (Activated) {
                            Cerr << "... captured cache request" << Endl;
                            CacheRequests.emplace_back(ev.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return PrevObserverFunc(ev);
            }

        private:
            TTestActorRuntime& Runtime;
            TTestActorRuntime::TEventObserver PrevObserverFunc;
            bool Installed = false;
            bool Activated = false;
            TVector<THolder<IEventHandle>> CacheRequests;
        } hooks(runtime);

        hooks.Install();
        Y_DEFER {
            hooks.Uninstall();
        };

        Cerr << "... rebooting node broker" << Endl;
        // runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_TRACE);
        runtime.Register(CreateTabletKiller(MakeNodeBrokerID()));
        hooks.WaitCacheRequests();

        // Open a new pipe
        auto pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());

        // Send an extend lease request while node broker is loading its state
        Cerr << "... sending extend lease request" << Endl;
        {
            TAutoPtr<TEvNodeBroker::TEvExtendLeaseRequest> event = new TEvNodeBroker::TEvExtendLeaseRequest;
            event->Record.SetNodeId(NODE1);
            runtime.SendToPipe(pipe, sender, event.Release(), 0);
        }

        // Simulate some sleep, enough for buggy code to handle the request
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Unblock state loading transaction
        hooks.Uninstall();
        hooks.ResendCacheRequests();

        Cerr << "... waiting for response" << Endl;
        {
            auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvExtendLeaseResponse>(sender);
            UNIT_ASSERT(reply);
            const auto &rec = reply->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), TStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNodeId(), NODE1);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().DebugString(), epoch.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(rec.GetExpire(), epoch.GetNextEnd());
        }

        // Wait until epoch expiration.
        Cerr << "... waiting for epoch update" << Endl;
        WaitForEpochUpdate(runtime, sender);
        epoch = CheckNodesList(runtime, sender, {NODE1}, {}, 3);
    }

    Y_UNIT_TEST(MinDynamicNodeIdShifted)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);
        // Register node NODE1.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        
        // Update config and restart NodeBroker
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId += 2;
        dnConfig->MaxDynamicNodeId += 2;
        RestartNodeBroker(runtime);

        // Register node NODE3.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE3, epoch.GetNextEnd());

        // Wait until epoch expiration.
        WaitForEpochUpdate(runtime, sender);
        epoch = GetEpoch(runtime, sender);

        // Check lease extension for both nodes.
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);
    }

    Y_UNIT_TEST(DoNotReuseDynnodeIdsBelowMinDynamicNodeId)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);
    
        // Register node NODE1.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        
        // Update config and restart NodeBroker
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId += 2;
        dnConfig->MaxDynamicNodeId += 2;
        RestartNodeBroker(runtime);
    
        // Wait until epoch expiration.
        WaitForEpochUpdate(runtime, sender);
        epoch = GetEpoch(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::OK);

        WaitForEpochUpdate(runtime, sender);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::OK);

        // Wait until node's lease expires
        WaitForEpochUpdate(runtime, sender);
        WaitForEpochUpdate(runtime, sender);
        WaitForEpochUpdate(runtime, sender);
        WaitForEpochUpdate(runtime, sender);
        epoch = GetEpoch(runtime, sender);

        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Register node NODE3.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE3, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(ResolveScopeIdForServerless)
    {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();
        auto epoch = GetEpoch(runtime, sender);
        ui32 txId = 100;

        // Create shared subdomain
        TSubDomainKey sharedSubdomainKey;
        do {
            auto modifyScheme = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            modifyScheme->Record.SetTxId(++txId);
            auto* transaction = modifyScheme->Record.AddTransaction();
            transaction->SetWorkingDir(DOMAIN_NAME);
            transaction->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExtSubDomain);
            auto* subdomain = transaction->MutableSubDomain();
            subdomain->SetName("SharedDB");
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, modifyScheme.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                sharedSubdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);

        // Check dynamic node in shared subdomain
        TKikimrScopeId sharedScopeId{sharedSubdomainKey.GetSchemeShard(), sharedSubdomainKey.GetPathId()};
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net",
                          "1.2.3.4", 1, 2, 3, 4, TStatus::OK, NODE1,
                          epoch.GetNextEnd(), false, "/dc-1/SharedDB",
                          sharedScopeId);

        // Create serverless subdomain that associated with shared
        TSubDomainKey serverlessSubdomainKey;
        do {
            auto modifyScheme = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            modifyScheme->Record.SetTxId(++txId);
            auto* transaction = modifyScheme->Record.AddTransaction();
            transaction->SetWorkingDir(DOMAIN_NAME);
            transaction->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExtSubDomain);
            auto* subdomain = transaction->MutableSubDomain();
            subdomain->SetName("ServerlessDB");
            subdomain->MutableResourcesDomainKey()->CopyFrom(sharedSubdomainKey);
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, modifyScheme.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                serverlessSubdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);
        
        // Check that dynamic node in serverless subdomain has shared scope id
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net",
                          "1.2.3.5", 1, 2, 3, 5, TStatus::OK, NODE2,
                          epoch.GetNextEnd(), false, "/dc-1/ServerlessDB",
                          sharedScopeId);
    }

    Y_UNIT_TEST(NodeNameExpiration)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4, { "/dc-1/my-database" });
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);

        // Register nodes for my-database
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        CheckRegistration(runtime, sender, "host2", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");
        CheckRegistration(runtime, sender, "host3", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE3, epoch.GetNextEnd(), "slot-2");

        // Wait until epoch expiration
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend lease for NODE1 and NODE3
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);

        // After this epoch update NODE2 is expired, but stil holds name
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend lease for NODE1 and NODE3
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);

        // Register one more node
        CheckRegistration(runtime, sender, "host4", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE4, epoch.GetNextEnd(), "slot-3");

        // After this epoch update NODE2 is removed and name is free 
        epoch = WaitForEpochUpdate(runtime, sender);

        // Register node using new host, it reuses name
        CheckRegistration(runtime, sender, "host5", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");
    }

    Y_UNIT_TEST(NodeNameReuseRestart)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4, { "/dc-1/my-database" });
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);

        // Register nodes for my-database
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        CheckRegistration(runtime, sender, "host2", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");

        // Restart
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        CheckRegistration(runtime, sender, "host2", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");

        // One more restart with different order
        CheckRegistration(runtime, sender, "host2", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
    }

    Y_UNIT_TEST(NodeNameReuseRestartWithHostChanges)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4, { "/dc-1/my-database" });
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);

        // Register nodes for my-database
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        CheckRegistration(runtime, sender, "host2", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");

        // Restart that caused the hosts to change
        CheckRegistration(runtime, sender, "host3", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE3, epoch.GetNextEnd(), "slot-2");
        CheckRegistration(runtime, sender, "host4", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE4, epoch.GetNextEnd(), "slot-3");

        // Wait until epoch expiration
        epoch = WaitForEpochUpdate(runtime, sender);

        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE4, TStatus::OK, epoch);

        // After this epoch update NODE1 and NODE2 are expired, but stil hold names
        epoch = WaitForEpochUpdate(runtime, sender);

        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE4, TStatus::OK, epoch);

         // After this epoch update NODE1 and NODE2 are removed
        epoch = WaitForEpochUpdate(runtime, sender);

        // One more restart that caused the hosts to change
        CheckRegistration(runtime, sender, "host5", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        CheckRegistration(runtime, sender, "host6", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");
    }

    Y_UNIT_TEST(NodeNameWithDifferentTenants)
    {
        TTestBasicRuntime runtime(8, false);

        Setup(runtime, 4, { "/dc-1/my-database" , "/dc-1/yet-another-database" });
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);

        // Register nodes for my-database
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        CheckRegistration(runtime, sender, "host2", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-1");

        // Register node for yet-another-database
        CheckRegistration(runtime, sender, "host3", 19001, "/dc-1/yet-another-database",
                          TStatus::OK, NODE3, epoch.GetNextEnd(), "slot-0");
        // Restart NODE1 to serve yet-another-database
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/yet-another-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-1");

        // Register one more node for my-database
        CheckRegistration(runtime, sender, "host4", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE4, epoch.GetNextEnd(), "slot-0");
        // Restart NODE1 to serve my-database
        CheckRegistration(runtime, sender, "host1", 19001, "/dc-1/my-database",
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-2");
    }
}

Y_UNIT_TEST_SUITE(TDynamicNameserverTest) {
    Y_UNIT_TEST(BasicFunctionality)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        // Register node NODE1.
        SetEpochDuration(runtime, sender, TDuration::Seconds(10));
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        // Try to resolve node.
        CheckResolveNode(runtime, sender, NODE1, "1.2.3.4");
        // Resolve unknown node the same domain.
        CheckResolveUnknownNode(runtime, sender, NODE2);
        // Move to the next epoch.
        WaitForEpochUpdate(runtime, sender);
        // Register node NODE2.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2);
        // Check nodes list.
        CheckNameserverNodesList(runtime, sender, 10);
        CheckNameserverDynamicNodesList(runtime, sender, 2,
                                        MakeICNodeInfo(NODE1, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4),
                                        MakeICNodeInfo(NODE2, "host2", 1001, "host2.host2.host2", "1.2.3.5", 1, 2, 3, 5));
        // Check NODE2 node is resolved.
        CheckResolveNode(runtime, sender, NODE2, "1.2.3.5");
        // Move to the next epoch. Node NODE1 should expire.
        WaitForEpochUpdate(runtime, sender);
        CheckResolveUnknownNode(runtime, sender, NODE1);
        CheckResolveNode(runtime, sender, NODE2, "1.2.3.5");
        CheckNameserverDynamicNodesList(runtime, sender, 1,
                                        MakeICNodeInfo(NODE2, "host2", 1001, "host2.host2.host2", "1.2.3.5", 1, 2, 3, 5));
        // Get existing static node.
        CheckGetNode(runtime, sender, runtime.GetNodeId(0), true);
        // Get unknown static node.
        CheckGetNode(runtime, sender, runtime.GetNodeId(7) + 1, false);
        // Get existing dynamic node.
        CheckGetNode(runtime, sender, NODE2, true);
        // Get unknown dynamic node.
        CheckGetNode(runtime, sender, 1057, false);
    }

    Y_UNIT_TEST(TestCacheUsage)
    {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        TVector<NKikimrNodeBroker::TListNodes> listRequests;
        TVector<NKikimrNodeBroker::TResolveNode> resolveRequests;

        auto logRequests = [&](TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvNodeBroker::EvListNodes)
                listRequests.push_back(event->Get<TEvNodeBroker::TEvListNodes>()->Record);
            else if (event->GetTypeRewrite() == TEvNodeBroker::EvResolveNode)
                resolveRequests.push_back(event->Get<TEvNodeBroker::TEvResolveNode>()->Record);
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        struct TIsEvUpdateEpoch {
            bool operator()(IEventHandle &ev) {
                if (ev.HasEvent()
                    && (dynamic_cast<TDynamicNameserver::TEvPrivate::TEvUpdateEpoch*>(ev.StaticCastAsLocal<IEventBase>())))
                    return true;
                return false;
            }
        };

        // Static assignemt with mixed slots should take place first.
        runtime.SetObserverFunc(logRequests);

        // Expect nameservice to request nodes list to cache current epoch.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvNodeBroker::EvNodesInfo, 1);
            runtime.DispatchEvents(options);
        }

        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        listRequests.clear();
        resolveRequests.clear();

        // This request should go to Node Broker.
        CheckResolveNode(runtime, sender, NODE1, "1.2.3.4");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 1);
        // The next request for the same node should use cache.
        CheckResolveNode(runtime, sender, NODE1, "1.2.3.4");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 1);
        // Unknown node resolve always should cause request to Node Broker.
        CheckResolveUnknownNode(runtime, sender, NODE2);
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 2);
        CheckResolveUnknownNode(runtime, sender, NODE2);
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 3);

        // Nodes list request should always cause request to Node Broker.
        CheckNameserverDynamicNodesList(runtime, sender, 1,
                                        MakeICNodeInfo(NODE1, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4));
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[0].GetCachedVersion(), 1);

        CheckNameserverDynamicNodesList(runtime, sender, 1,
                                        MakeICNodeInfo(NODE1, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4));
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[1].GetCachedVersion(), 2);

        auto epoch = GetEpoch(runtime, sender);
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));
        listRequests.clear();

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TIsEvUpdateEpoch(), 1);
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[0].GetMinEpoch(), 2);

        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2);

        CheckNameserverDynamicNodesList(runtime, sender, 2,
                                        MakeICNodeInfo(NODE1, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4),
                                        MakeICNodeInfo(NODE2, "host2", 1001, "host2.host2.host2", "1.2.3.5", 1, 2, 3, 5));
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[1].GetCachedVersion(), 3);

        // This node should be cached.
        CheckResolveNode(runtime, sender, NODE2, "1.2.3.5");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 3);

        // Go to the next epoch.
        epoch = GetEpoch(runtime, sender);
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));
        listRequests.clear();

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvNodeBroker::EvNodesInfo, 1);
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[0].GetMinEpoch(), 3);

        // Both live and expired nodes should be resolved from the cache.
        CheckResolveUnknownNode(runtime, sender, NODE1);
        CheckResolveNode(runtime, sender, NODE2, "1.2.3.5");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 3);
    }
}

Y_UNIT_TEST_SUITE(TSlotIndexesPoolTest) {
    Y_UNIT_TEST(Init)
    {
        TSlotIndexesPool pool;
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 64);

        for (size_t i = 0; i < pool.Capacity(); ++i) {
            UNIT_ASSERT(!pool.IsAcquired(i));
        }
    }

    Y_UNIT_TEST(Basic)
    {
        TSlotIndexesPool pool;
        
        pool.Acquire(10);
        UNIT_ASSERT(pool.IsAcquired(10));

        pool.Acquire(45);
        UNIT_ASSERT(pool.IsAcquired(45));

        pool.AcquireLowestFreeIndex();
        UNIT_ASSERT(pool.IsAcquired(0));

        pool.AcquireLowestFreeIndex();
        UNIT_ASSERT(pool.IsAcquired(1));

        pool.Release(0);
        UNIT_ASSERT(!pool.IsAcquired(0));

        pool.AcquireLowestFreeIndex();
        UNIT_ASSERT(pool.IsAcquired(0));

        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 64);

        pool.ReleaseAll();
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 64);
    }

    Y_UNIT_TEST(Expansion)
    {
        TSlotIndexesPool pool;
        for (size_t i = 0; i < pool.Capacity(); ++i) {
            pool.Acquire(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 64);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 64);

        pool.AcquireLowestFreeIndex();

        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 65);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 192);
        for (size_t i = pool.Size(); i < pool.Capacity(); ++i) {
            UNIT_ASSERT(!pool.IsAcquired(i));
        }
    }

    Y_UNIT_TEST(Ranges)
    {
        TSlotIndexesPool pool;

        pool.Acquire(63);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 64);

        pool.Release(63);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 64);

        pool.Acquire(64);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 128);

        for (size_t i = 0; i < pool.Capacity(); ++i) {
            if (i == 64) {
                UNIT_ASSERT(pool.IsAcquired(i));
            } else {
                UNIT_ASSERT(!pool.IsAcquired(i));
            }
        }

        pool.Release(128);
        pool.Release(200);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(pool.Capacity(), 128);
    }
}

} // NKikimr
