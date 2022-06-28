#include "node_broker_impl.h"
#include "dynamic_nameserver_impl.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/actors/interconnect/events_local.h>
#include <library/cpp/actors/interconnect/interconnect_impl.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>

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

void SetupLogging(TTestActorRuntime& runtime)
{
    NActors::NLog::EPriority priority = ENABLE_DETAILED_NODE_BROKER_LOG ? NLog::PRI_TRACE : NLog::PRI_ERROR;

    runtime.SetLogPriority(NKikimrServices::NODE_BROKER, priority);
}

THashMap<ui32, TIntrusivePtr<TNodeWardenConfig>> NodeWardenConfigs;

void SetupServices(TTestActorRuntime &runtime,
                   ui32 maxDynNodes,
                   bool singleDomainMode)
{
    const ui32 domainsNum = 1;
    const ui32 disksInDomain = 1;

    // setup domain info
    TAppPrepare app;

    app.ClearDomainsAndHive();
    app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());

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
        google::protobuf::TextFormat::ParseFromString(staticConfig, &nodeWardenConfig->ServiceSet);

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
            nodeWardenConfig->ServiceSet.MutablePDisks(0)->SetPath(pDiskPath);
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
                        sectorMap);
        }

        NodeWardenConfigs[nodeIndex] = nodeWardenConfig;

        SetupBSNodeWarden(runtime, nodeIndex, nodeWardenConfig);
        SetupNodeWhiteboard(runtime, nodeIndex);
        SetupTabletResolver(runtime, nodeIndex);
    }

    runtime.Initialize(app.Unwrap());

    runtime.GetAppData().DynamicNameserviceConfig = new TDynamicNameserviceConfig;
    auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
    dnConfig->MaxStaticNodeId = 1023;
    dnConfig->MaxDynamicNodeId = 1024 + (singleDomainMode ? (maxDynNodes - 1) : 32 * (maxDynNodes - 1));
    runtime.GetAppData().FeatureFlags.SetEnableNodeBrokerSingleDomainMode(singleDomainMode);

    if (!runtime.IsRealThreads()) {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvLocalRecoveryDone,
                                                                             domainsNum * disksInDomain));
        runtime.DispatchEvents(options);
    }

    auto aid = CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeNodeBrokerID(0), TTabletTypes::NodeBroker), &CreateNodeBroker);
    runtime.EnableScheduleForActor(aid, true);
}

void SetConfig(TTestActorRuntime& runtime,
               TActorId sender,
               const NKikimrNodeBroker::TConfig &config)
{
    auto event = MakeHolder<TEvNodeBroker::TEvSetConfigRequest>();
    event->Record.MutableConfig()->CopyFrom(config);
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
           bool singleDomainMode = false)
{
    using namespace NMalloc;
    TMallocInfo mallocInfo = MallocInfo();
    mallocInfo.SetParam("FillMemoryOnAllocation", "false");

    auto scheduledFilter = [](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
        if (event->HasEvent()
            && (dynamic_cast<TDynamicNameserver::TEvPrivate::TEvUpdateEpoch*>(event->GetBase())))
            return false;
        return TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
    };
    runtime.SetScheduledEventFilter(scheduledFilter);

    SetupLogging(runtime);
    SetupServices(runtime, maxDynNodes, singleDomainMode);
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
                       bool fixed = false)
{
    auto event = MakeRegistrationRequest(host, port, resolveHost, address, dc, room, rack, body, fixed);
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
    }
}

NKikimrNodeBroker::TEpoch GetEpoch(TTestActorRuntime &runtime,
                                   TActorId sender)
{
    TAutoPtr<TEvNodeBroker::TEvListNodes> event = new TEvNodeBroker::TEvListNodes;
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
                    && (dynamic_cast<TNodeBroker::TEvPrivate::TEvUpdateEpoch*>(ev.GetBase())))
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
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
    runtime.SendToPipe(MakeNodeBrokerID(0), sender, event.Release(), 0, GetPipeConfigWithRetries());

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
    runtime.Register(CreateTabletKiller(MakeNodeBrokerID(0)));
    runtime.DispatchEvents(options);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TNodeBrokerTest) {
    Y_UNIT_TEST(BasicFunctionality)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);
        // Register node 1024.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, epoch.GetNextEnd());
        // Check node 1024 exists.
        CheckNodeInfo(runtime, sender, 1024, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        // Nodes list now has 1 node.
        CheckNodesList(runtime, sender, {1024}, {}, 1);
        // There is no node 1025.
        CheckNodeInfo(runtime, sender, 1025, TStatus::WRONG_REQUEST);
        // Register node 1056.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1056, epoch.GetNextEnd());

        // Wait until epoch expiration.
        WaitForEpochUpdate(runtime, sender);
        epoch = CheckNodesList(runtime, sender, {1024, 1056}, {}, 2);

        // Extend lease for node 1024.
        CheckLeaseExtension(runtime, sender, 1024, TStatus::OK, epoch);

        // Kill tablet, wait for node 1056 expiration and check state is restored correctly.
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));

        RestartNodeBroker(runtime);

        epoch = CheckNodesList(runtime, sender, {1024}, {1056}, 3);
        CheckNodeInfo(runtime, sender, 1024, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetEnd());
        CheckNodeInfo(runtime, sender, 1056, TStatus::WRONG_REQUEST);

        // Register node 1088.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::OK, 1088, epoch.GetNextEnd());
        // Register node with existing lease, this should extend lease to the next epoch.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, epoch.GetNextEnd());
        // Registration of existing node with different location.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 5, TStatus::WRONG_REQUEST, 1024);
        // Registration of existing node with different address, expect new node id.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.14",
                          1, 2, 3, 4, TStatus::OK, 1120, epoch.GetNextEnd());
        // There should be no more free IDs.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        // Extend lease for expired node.
        CheckLeaseExtension(runtime, sender, 1056, TStatus::WRONG_REQUEST);
        // Extend lease for unknown node.
        CheckLeaseExtension(runtime, sender, 1025, TStatus::WRONG_REQUEST);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {1024, 1088, 1120}, {}, 4);

        // Register node and re-use 1056 node ID.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::OK, 1056, epoch.GetNextEnd());
        epoch.SetVersion(epoch.GetVersion() + 1);
        CheckLeaseExtension(runtime, sender, 1024, TStatus::OK, epoch);

        WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {1024, 1056}, {1088, 1120}, 5);

        WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {1024, 1056}, 6);
    }

    Y_UNIT_TEST(FixedNodeId)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        // Register node 1024 with fixed ID.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, Max<ui64>(), true);
        epoch.SetVersion(epoch.GetVersion() + 1);
        // Check node 1024 exists and has infinite lease.
        CheckNodeInfo(runtime, sender, 1024, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, Max<ui64>());
        // Lease extension should work fine and report infinite lease.
        CheckLeaseExtension(runtime, sender, 1024, TStatus::OK, epoch, true);
        // Register node 1024 without fixed ID option. This shouldn't unfix ID.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, Max<ui64>(), false);
        // Check node still has infinite lease.
        CheckNodeInfo(runtime, sender, 1024, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, Max<ui64>());

        // Register node 1056.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1056, epoch.GetNextEnd(), false);
        epoch.SetVersion(epoch.GetVersion() + 1);
        // Now register it again but with fixed ID.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1056, Max<ui64>(), true);
        // Check node 1056 has infinite lease and ping doesn't affect it.
        CheckNodeInfo(runtime, sender, 1056, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                      1, 2, 3, 5, Max<ui64>());
        CheckLeaseExtension(runtime, sender, 1056, TStatus::OK, epoch, true);
        CheckNodeInfo(runtime, sender, 1056, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                      1, 2, 3, 5, Max<ui64>());
        // Check tegular registration doesn't affect infinite lease.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1056, Max<ui64>(), false);
        CheckLeaseExtension(runtime, sender, 1056, TStatus::OK, epoch, true);
    }

    Y_UNIT_TEST(TestListNodes)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 10);
        TActorId sender = runtime.AllocateEdgeActor();

        WaitForEpochUpdate(runtime, sender);
        WaitForEpochUpdate(runtime, sender);
        auto epoch = GetEpoch(runtime, sender);
        // Register node 1024.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, epoch.GetNextEnd());
        epoch = CheckFilteredNodesList(runtime, sender, {1024}, {}, epoch.GetId(), 0);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {1024}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {1024}, {}, 0, epoch.GetVersion() + 1);
        auto epoch1 = CheckFilteredNodesList(runtime, sender, {1024}, {}, epoch.GetId() - 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(epoch1.GetId(), epoch.GetId());
        epoch1 = CheckFilteredNodesList(runtime, sender, {1024}, {}, epoch.GetId() + 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(epoch1.GetId(), epoch.GetId() + 1);
        epoch1 = CheckFilteredNodesList(runtime, sender, {}, {1024}, epoch.GetId() + 2, 0);
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
            1024, 1056, 1088, 1120, 1152,
            1184, 1216, 1248, 1280, 1312
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
                    runtime.Register(CreateTabletKiller(MakeNodeBrokerID(0)));
                    runtime.DispatchEvents(options);
                }
                break;
            }
        }
    }

    Y_UNIT_TEST(BannedIds)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 10);
        TActorId sender = runtime.AllocateEdgeActor();

        SetBannedIds(runtime, sender, {{1025, 1280}});

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);
        // Register node 1024.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, epoch.GetNextEnd());
        // Register node 1312.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1312, epoch.GetNextEnd());
        // No more free IDs.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::ERROR_TEMP);

        SetBannedIds(runtime, sender, {{1056, 1183}, {1185, 1311}});
        // Register node 1184.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::OK, 1184, epoch.GetNextEnd());
        // No more free IDs.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        epoch = GetEpoch(runtime, sender);

        // Now ban registered node and check lease extension.
        SetBannedIds(runtime, sender, {{1024, 1183}, {1185, 1311}});
        CheckLeaseExtension(runtime, sender, 1024, TStatus::WRONG_REQUEST);
        CheckLeaseExtension(runtime, sender, 1184, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, 1312, TStatus::OK, epoch);

        CheckNodeInfo(runtime, sender, 1024, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Wait until node 1024 expires.
        WaitForEpochUpdate(runtime, sender);
        epoch = GetEpoch(runtime, sender);
        CheckLeaseExtension(runtime, sender, 1184, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, 1312, TStatus::OK, epoch);
        WaitForEpochUpdate(runtime, sender);

        CheckNodeInfo(runtime, sender, 1024, TStatus::WRONG_REQUEST);
        // No more free IDs still.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        RestartNodeBroker(runtime);

        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);
    }

    Y_UNIT_TEST(SingleDomainModeBannedIds) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, 10, /* single domain */ true);
        TActorId sender = runtime.AllocateEdgeActor();

        SetBannedIds(runtime, sender, {{1025, 1032}});

        // There should be no dynamic nodes initially.
        auto epoch = GetEpoch(runtime, sender);

        // Register node 1024.
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024, epoch.GetNextEnd());
        // Register node 1033.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1033, epoch.GetNextEnd());
        // No more free IDs.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::ERROR_TEMP);

        SetBannedIds(runtime, sender, {{1025, 1029}, {1031, 1032}});
        // Register node 1030.
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::OK, 1030, epoch.GetNextEnd());
        // No more free IDs.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        epoch = GetEpoch(runtime, sender);

        // Now ban registered node and check lease extension.
        SetBannedIds(runtime, sender, {{1024, 1029}, {1031, 1032}});
        CheckLeaseExtension(runtime, sender, 1024, TStatus::WRONG_REQUEST);
        CheckLeaseExtension(runtime, sender, 1030, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, 1033, TStatus::OK, epoch);

        CheckNodeInfo(runtime, sender, 1024, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Wait until node 1024 expires.
        WaitForEpochUpdate(runtime, sender);
        epoch = GetEpoch(runtime, sender);
        CheckLeaseExtension(runtime, sender, 1030, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, 1033, TStatus::OK, epoch);
        WaitForEpochUpdate(runtime, sender);

        CheckNodeInfo(runtime, sender, 1024, TStatus::WRONG_REQUEST);
        // No more free IDs still.
        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);

        RestartNodeBroker(runtime);

        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::ERROR_TEMP);
    }
}

Y_UNIT_TEST_SUITE(TDynamicNameserverTest) {
    Y_UNIT_TEST(BasicFunctionality)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        // Register node 1024.
        SetEpochDuration(runtime, sender, TDuration::Seconds(10));
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, 1024);
        // Try to resolve node.
        CheckResolveNode(runtime, sender, 1024, "1.2.3.4");
        // Resolve unknown node in another domain.
        CheckResolveUnknownNode(runtime, sender, 1025);
        // Resolve unknown node the same domain.
        CheckResolveUnknownNode(runtime, sender, 1056);
        // Move to the next epoch.
        WaitForEpochUpdate(runtime, sender);
        // Register node 1056.
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, 1056);
        // Check nodes list.
        CheckNameserverNodesList(runtime, sender, 10);
        CheckNameserverDynamicNodesList(runtime, sender, 2,
                                        MakeICNodeInfo(1024, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4),
                                        MakeICNodeInfo(1056, "host2", 1001, "host2.host2.host2", "1.2.3.5", 1, 2, 3, 5));
        // Check 1056 node is resolved.
        CheckResolveNode(runtime, sender, 1056, "1.2.3.5");
        // Move to the next epoch. Node 1024 should expire.
        WaitForEpochUpdate(runtime, sender);
        CheckResolveUnknownNode(runtime, sender, 1024);
        CheckResolveNode(runtime, sender, 1056, "1.2.3.5");
        CheckNameserverDynamicNodesList(runtime, sender, 1,
                                        MakeICNodeInfo(1056, "host2", 1001, "host2.host2.host2", "1.2.3.5", 1, 2, 3, 5));
        // Get existing static node.
        CheckGetNode(runtime, sender, runtime.GetNodeId(0), true);
        // Get unknown static node.
        CheckGetNode(runtime, sender, runtime.GetNodeId(7) + 1, false);
        // Get existing dynamic node.
        CheckGetNode(runtime, sender, 1056, true);
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

        auto logRequests = [&](TTestActorRuntimeBase &, TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvNodeBroker::EvListNodes)
                listRequests.push_back(event->Get<TEvNodeBroker::TEvListNodes>()->Record);
            else if (event->GetTypeRewrite() == TEvNodeBroker::EvResolveNode)
                resolveRequests.push_back(event->Get<TEvNodeBroker::TEvResolveNode>()->Record);
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        struct TIsEvUpdateEpoch {
            bool operator()(IEventHandle &ev) {
                if (ev.HasEvent()
                    && (dynamic_cast<TDynamicNameserver::TEvPrivate::TEvUpdateEpoch*>(ev.GetBase())))
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
                          1, 2, 3, 4, TStatus::OK, 1024);

        listRequests.clear();
        resolveRequests.clear();

        // This request should go to Node Broker.
        CheckResolveNode(runtime, sender, 1024, "1.2.3.4");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 1);
        // The next request for the same node should use cache.
        CheckResolveNode(runtime, sender, 1024, "1.2.3.4");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 1);
        // Unknown node resolve always should cause request to Node Broker.
        CheckResolveUnknownNode(runtime, sender, 1056);
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 2);
        CheckResolveUnknownNode(runtime, sender, 1056);
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 3);

        // Nodes list request should always cause request to Node Broker.
        CheckNameserverDynamicNodesList(runtime, sender, 1,
                                        MakeICNodeInfo(1024, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4));
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[0].GetCachedVersion(), 1);

        CheckNameserverDynamicNodesList(runtime, sender, 1,
                                        MakeICNodeInfo(1024, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4));
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
                          1, 2, 3, 5, TStatus::OK, 1056);

        CheckNameserverDynamicNodesList(runtime, sender, 2,
                                        MakeICNodeInfo(1024, "host1", 1001, "host1.host1.host1", "1.2.3.4", 1, 2, 3, 4),
                                        MakeICNodeInfo(1056, "host2", 1001, "host2.host2.host2", "1.2.3.5", 1, 2, 3, 5));
        UNIT_ASSERT_VALUES_EQUAL(listRequests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(listRequests[1].GetCachedVersion(), 3);

        // This node should be cached.
        CheckResolveNode(runtime, sender, 1056, "1.2.3.5");
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
        CheckResolveUnknownNode(runtime, sender, 1024);
        CheckResolveNode(runtime, sender, 1056, "1.2.3.5");
        UNIT_ASSERT_VALUES_EQUAL(resolveRequests.size(), 3);
    }
}

} // NKikimr
