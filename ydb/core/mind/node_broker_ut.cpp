#include "node_broker_impl.h"
#include "node_broker__scheme.h"
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
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/system/hostname.h>

// ad-hoc test parametrization support: only for single boolean flag
// taken from ydb/core/ut/common/kqp_ut_common.h:Y_UNIT_TEST_TWIN
//TODO: introduce general support for test parametrization?
#define Y_UNIT_TEST_FLAG(N, OPT)                                                                                   \
    template<bool OPT> void Test##N(NUnitTest::TTestContext&);                                                           \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(#N "-" #OPT "-false", static_cast<void (*)(NUnitTest::TTestContext&)>(&Test##N<false>), false); \
            TCurrentTest::AddTest(#N "-" #OPT "-true", static_cast<void (*)(NUnitTest::TTestContext&)>(&Test##N<true>), false);   \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template<bool OPT>                                                                                             \
    void Test##N(NUnitTest::TTestContext&)

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
    runtime.SetLogPriority(NKikimrServices::NAMESERVICE, priority);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, priority);
}

THashMap<ui32, TIntrusivePtr<TNodeWardenConfig>> NodeWardenConfigs;

void SetupServices(TTestActorRuntime &runtime,
                   ui32 maxDynNodes,
                   bool enableNodeBrokerDeltaProtocol)
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
        NSharedCache::TSharedCacheConfig sharedCacheConfig;
        sharedCacheConfig.SetMemoryLimit(0);
        SetupSharedPageCache(runtime, nodeIndex, sharedCacheConfig);
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
    runtime.GetAppData().FeatureFlags.SetEnableNodeBrokerDeltaProtocol(enableNodeBrokerDeltaProtocol);

    if (!runtime.IsRealThreads()) {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvLocalRecoveryDone,
                                                                             domainsNum * disksInDomain));
        runtime.DispatchEvents(options);
    }

    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
    BootFakeCoordinator(runtime, TTestTxConfig::Coordinator, MakeIntrusive<TFakeCoordinator::TState>());

    ui32 connectedNameservers = 0;
    auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvTabletPipe::EvClientConnected) {
            auto* connectedEv = ev->Get<TEvTabletPipe::TEvClientConnected>();
            if (connectedEv->TabletId == MakeNodeBrokerID() && connectedEv->Status == NKikimrProto::OK) {
                ++connectedNameservers;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    auto aid = CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeNodeBrokerID(), TTabletTypes::NodeBroker), &CreateNodeBroker);
    runtime.EnableScheduleForActor(aid, true);

    runtime.WaitFor("nameservers are connected", [&]{ return connectedNameservers >= runtime.GetNodeCount(); });
    runtime.SetObserverFunc(prevObserver);
}

void AsyncSetConfig(TTestActorRuntime& runtime,
    TActorId sender,
    const NKikimrNodeBroker::TConfig &config)
{
    auto event = MakeHolder<TEvNodeBroker::TEvSetConfigRequest>();
    event->Record.MutableConfig()->CopyFrom(config);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());
}

void CheckAsyncSetConfig(TTestActorRuntime& runtime)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodeBroker::TEvSetConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), TStatus::OK);
}

void SetConfig(TTestActorRuntime& runtime,
               TActorId sender,
               const NKikimrNodeBroker::TConfig &config)
{
    AsyncSetConfig(runtime, sender, config);
    CheckAsyncSetConfig(runtime);
}

void SetEpochDuration(TTestActorRuntime& runtime,
                      TActorId sender,
                      TDuration lease)
{
    NKikimrNodeBroker::TConfig config;
    config.SetEpochDuration(lease.GetValue());
    SetConfig(runtime, sender, config);
}

void AsyncSetBannedIds(TTestActorRuntime& runtime,
    TActorId sender,
    const TVector<std::pair<ui32, ui32>> ids)
{
    NKikimrNodeBroker::TConfig config;
    for (auto &pr : ids) {
        auto &entry = *config.AddBannedNodeIds();
        entry.SetFrom(pr.first);
        entry.SetTo(pr.second);
    }
    AsyncSetConfig(runtime, sender, config);
}

void SetBannedIds(TTestActorRuntime& runtime,
                  TActorId sender,
                  const TVector<std::pair<ui32, ui32>> ids)
{
    AsyncSetBannedIds(runtime, sender, ids);
    CheckAsyncSetConfig(runtime);
}

void Setup(TTestActorRuntime& runtime,
           ui32 maxDynNodes = 3,
           const TVector<TString>& databases = {},
           bool enableNodeBrokerDeltaProtocol = false)
{
    using namespace NMalloc;
    TMallocInfo mallocInfo = MallocInfo();
    mallocInfo.SetParam("FillMemoryOnAllocation", "false");

    auto scheduledFilter = [](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
        if (event->HasEvent()
            && (event->Type == TDynamicNameserver::TEvPrivate::TEvUpdateEpoch::EventType 
                || event->Type == TEvents::TSystem::Wakeup))
            return false;
        return TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
    };
    runtime.SetScheduledEventFilter(scheduledFilter);

    SetupLogging(runtime);
    SetupServices(runtime, maxDynNodes, enableNodeBrokerDeltaProtocol);

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

void SetupWithDeltaProtocol(TTestActorRuntime& runtime, bool enableNodeBrokerDeltaProtocol)
{
    Setup(runtime, 3, {}, enableNodeBrokerDeltaProtocol);
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
    if (dc) {
        loc.SetDataCenter(ToString(dc));
    }
    if (room) {
        loc.SetModule(ToString(room));
    }
    if (rack) {
        loc.SetRack(ToString(rack));
    }
    if (body) {
        loc.SetUnit(ToString(body));
    }
    event->Record.SetFixedNodeId(fixed);
    event->Record.SetPath(path);
    return event;
}

void AsyncRegistration(TTestActorRuntime &runtime,
                       TActorId sender,
                       const TString &host,
                       ui16 port,
                       const TString &resolveHost,
                       const TString &address,
                       ui64 dc = 0,
                       ui64 room = 0,
                       ui64 rack = 0,
                       ui64 body = 0,
                       bool fixed = false,
                       const TString &path = DOMAIN_NAME)
{
    auto event = MakeRegistrationRequest(host, port, resolveHost, address, path, dc, room, rack, body, fixed);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());
    // To prevent registration reordering due to scheme cache responses
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvTxProxySchemeCache::EvNavigateKeySetResult);
    runtime.DispatchEvents(options);
}

void CheckAsyncRegistration(TTestActorRuntime &runtime,
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
                            const TMaybe<TKikimrScopeId> &scopeId = {},
                            const TString &name = "")
{
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
        if (dc)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetDataCenter(), ToString(dc));
        if (room)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetModule(), ToString(room));
        if (rack)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetRack(), ToString(rack));
        if (body)
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
    AsyncRegistration(runtime, sender, host, port, resolveHost, address, dc, room, rack, body, fixed, path);
    CheckAsyncRegistration(runtime, host, port, resolveHost, address, dc, room, rack, body, code, nodeId, expire, scopeId, name);
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

THolder<TEvNodeBroker::TEvGracefulShutdownRequest>
MakeEventGracefulShutdown (ui32 nodeId) 
{
    auto eventGracefulShutdown = MakeHolder<TEvNodeBroker::TEvGracefulShutdownRequest>();
    eventGracefulShutdown->Record.SetNodeId(nodeId);
    return eventGracefulShutdown;
}

void AsyncGracefulShutdown(TTestActorRuntime &runtime,
    TActorId sender,
    ui32 nodeId)
{
    auto eventGracefulShutdown = MakeEventGracefulShutdown(nodeId);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, eventGracefulShutdown.Release(), 0, GetPipeConfigWithRetries());
}

void CheckAsyncGracefulShutdown(TTestActorRuntime &runtime)
{
    TAutoPtr<IEventHandle> handle;
    auto replyGracefulShutdown = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvGracefulShutdownResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(replyGracefulShutdown->Record.GetStatus().GetCode(), TStatus::OK);
}

void CheckGracefulShutdown(TTestActorRuntime &runtime,
                           TActorId sender,
                           ui32 nodeId)
{
    AsyncGracefulShutdown(runtime, sender, nodeId);
    CheckAsyncGracefulShutdown(runtime);
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

void MoveToNextEpoch(TTestActorRuntime &runtime,
                     const NKikimrNodeBroker::TEpoch& epoch)
{
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

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TIsEvUpdateEpoch(), 1);
            runtime.DispatchEvents(options);
        }

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(NKikimr::TEvents::TEvFlushLog::EventType, 1);
            runtime.DispatchEvents(options);
        }
    }
}

NKikimrNodeBroker::TEpoch WaitForEpochUpdate(TTestActorRuntime &runtime,
                                             TActorId sender,
                                             NKikimrNodeBroker::TEpoch& epoch)
{
    ui64 reqEpoch = epoch.GetId() + 1;
    auto nextEpoch = epoch;
    while (nextEpoch.GetId() != reqEpoch) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NKikimr::TEvents::TEvFlushLog::EventType, 1);
        runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        nextEpoch = GetEpoch(runtime, sender);
    }
    return nextEpoch;
}

NKikimrNodeBroker::TEpoch WaitForEpochUpdate(TTestActorRuntime &runtime,
                                             TActorId sender)
{
    auto epoch = GetEpoch(runtime, sender);
    MoveToNextEpoch(runtime, epoch);
    return WaitForEpochUpdate(runtime, sender, epoch);
}

void CheckNodesListResponse(const NKikimrNodeBroker::TNodesInfo &rec,
                            TMultiSet<ui64> ids,
                            TMultiSet<ui64> expiredIds)
{
    UNIT_ASSERT_VALUES_EQUAL(rec.NodesSize(), ids.size());
    for (auto &node : rec.GetNodes()) {
        UNIT_ASSERT_C(ids.contains(node.GetNodeId()), node.GetNodeId());
        ids.erase(ids.find(node.GetNodeId()));
    }
    UNIT_ASSERT_VALUES_EQUAL(rec.ExpiredNodesSize(), expiredIds.size());
    for (auto &node : rec.GetExpiredNodes()) {
        UNIT_ASSERT_C(expiredIds.contains(node.GetNodeId()), node.GetNodeId());
        expiredIds.erase(expiredIds.find(node.GetNodeId()));
    }
}

NKikimrNodeBroker::TEpoch CheckFilteredNodesList(TTestActorRuntime &runtime,
                                                 TActorId sender,
                                                 TMultiSet<ui64> ids,
                                                 TMultiSet<ui64> expiredIds,
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

NKikimrNodeBroker::TNodesInfo ListNodes(TTestActorRuntime &runtime, TActorId sender)
{
    TAutoPtr<TEvNodeBroker::TEvListNodes> event = new TEvNodeBroker::TEvListNodes;
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvNodesInfo>(handle);
    UNIT_ASSERT(reply);
    return reply->GetRecord();
}

NKikimrNodeBroker::TEpoch CheckNodesList(TTestActorRuntime &runtime,
                                         TActorId sender,
                                         TSet<ui64> ids,
                                         TSet<ui64> expiredIds,
                                         ui64 epoch)
{
    const auto &rec = ListNodes(runtime, sender);
    TSet<ui64> recIds;
    TSet<ui64> recExpiredIds;
    for (const auto& node : rec.GetNodes()) {
        recIds.insert(node.GetNodeId());
    }
    for (const auto& node : rec.GetExpiredNodes()) {
        recExpiredIds.insert(node.GetNodeId());
    }
    UNIT_ASSERT_VALUES_EQUAL(recIds, ids);
    UNIT_ASSERT_VALUES_EQUAL(recExpiredIds, expiredIds);
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
    if (dc) {
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetDataCenter(), ToString(dc));
    }
    if (room) {
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetModule(), ToString(room));
    }
    if (rack) {
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetRack(), ToString(rack));
    }
    if (body) {
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetLocation().GetUnit(), ToString(body));
    }
    UNIT_ASSERT_VALUES_EQUAL(rec.GetNode().GetExpire(), expire);
}

void AsyncLeaseExtension(TTestActorRuntime &runtime,
                              TActorId sender,
                              ui32 nodeId)
{
    TAutoPtr<TEvNodeBroker::TEvExtendLeaseRequest> event = new TEvNodeBroker::TEvExtendLeaseRequest;
    event->Record.SetNodeId(nodeId);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, GetPipeConfigWithRetries());
}

void CheckAsyncLeaseExtension(TTestActorRuntime &runtime,
                              ui32 nodeId,
                              TStatus::ECode code,
                              const NKikimrNodeBroker::TEpoch &epoch,
                              bool fixed = false)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvExtendLeaseResponse>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
    if (code == TStatus::OK) {
        UNIT_ASSERT_VALUES_EQUAL(rec.GetNodeId(), nodeId);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetId(), epoch.GetId());
        UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetStart(), epoch.GetStart());
        UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetEnd(), epoch.GetEnd());
        UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetNextEnd(), epoch.GetNextEnd());
        if (fixed)
            UNIT_ASSERT_VALUES_EQUAL(rec.GetExpire(), Max<ui64>());
        else
            UNIT_ASSERT_VALUES_EQUAL(rec.GetExpire(), epoch.GetNextEnd());
    }
}

void CheckLeaseExtension(TTestActorRuntime &runtime,
                         TActorId sender,
                         ui32 nodeId,
                         TStatus::ECode code,
                         const NKikimrNodeBroker::TEpoch &epoch = {},
                         bool fixed = false)
{
    AsyncLeaseExtension(runtime, sender, nodeId);
    CheckAsyncLeaseExtension(runtime, nodeId, code, epoch, fixed);
}

struct TUpdate {
    enum class EType : ui8 {
        Unknown,
        Updated,
        Removed,
        Expired,
    };

    TUpdate(ui32 nodeId, EType type = EType::Updated)
        : NodeId(nodeId)
        , Type(type)
    {}

    TUpdate(const NKikimrNodeBroker::TUpdateNode &proto) {
        switch (proto.GetUpdateTypeCase()) {
            case TUpdateNode::kNode:
                Type = EType::Updated;
                NodeId = proto.GetNode().GetNodeId();
                break;
            case TUpdateNode::kExpiredNode:
                Type = EType::Expired;
                NodeId = proto.GetExpiredNode();
                break;
            case TUpdateNode::kRemovedNode:
                Type = EType::Removed;
                NodeId = proto.GetRemovedNode();
                break;
            case TUpdateNode::UPDATETYPE_NOT_SET:
                break;
        }
    }

    bool operator==(const TUpdate& other) const = default;

    ui32 NodeId = 0;
    EType Type = EType::Unknown;
};

TUpdate E(ui32 nodeId) {
    return TUpdate(nodeId, TUpdate::EType::Expired);
}

TUpdate R(ui32 nodeId) {
    return TUpdate(nodeId, TUpdate::EType::Removed);
}

void CheckNodesUpdate(TTestActorRuntime &runtime,
                     const TVector<TUpdate> &updates,
                     ui64 seqNo,
                     const NKikimrNodeBroker::TEpoch &epoch)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvUpdateNodes>(handle);
    UNIT_ASSERT(reply);
    const auto &rec = reply->GetRecord();
    UNIT_ASSERT_VALUES_EQUAL(rec.GetSeqNo(), seqNo);
    UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().DebugString(), epoch.DebugString());
    UNIT_ASSERT_VALUES_EQUAL_C(rec.UpdatesSize(), updates.size(), TStringBuilder()
        << "expected: [" << JoinSeq(", ", updates) << "]"
        << ", actual: [" << JoinSeq(", ", rec.GetUpdates()) << "]");
    for (size_t i = 0; i < updates.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(updates[i], rec.GetUpdates(i));
    }
}

void CheckSyncNodes(TTestActorRuntime &runtime,
                    TActorId sender,
                    const TVector<TUpdate> &updates,
                    ui64 seqNo,
                    TActorId pipe,
                    const NKikimrNodeBroker::TEpoch &epoch)
{
    TAutoPtr<TEvNodeBroker::TEvSyncNodesRequest> event = new TEvNodeBroker::TEvSyncNodesRequest;
    event->Record.SetSeqNo(seqNo);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, {}, pipe);

    TBlockEvents<TEvNodeBroker::TEvUpdateNodes> block(runtime);
    if (!updates.empty()) {
        block.Stop();
        block.Unblock();
        CheckNodesUpdate(runtime, updates, seqNo, epoch);
    }

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvNodeBroker::TEvSyncNodesResponse>(handle);
    UNIT_ASSERT(reply);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSeqNo(), seqNo);
    UNIT_ASSERT(block.empty());
}

void SubscribeToNodesUpdates(TTestActorRuntime &runtime, TActorId sender, TActorId pipe, ui64 version, ui64 seqNo)
{
    TAutoPtr<TEvNodeBroker::TEvSubscribeNodesRequest> event = new TEvNodeBroker::TEvSubscribeNodesRequest;
    event->Record.SetCachedVersion(version);
    event->Record.SetSeqNo(seqNo);
    runtime.SendToPipe(MakeNodeBrokerID(), sender, event.Release(), 0, {}, pipe);
}

void CheckUpdateNodesLog(TTestActorRuntime &runtime,
                         TActorId sender,
                         const TVector<TUpdate> &updates,
                         ui64 version,
                         const NKikimrNodeBroker::TEpoch &epoch)
{
    static ui32 seqNo = 1;
    TActorId pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
    SubscribeToNodesUpdates(runtime, sender, pipe, version, ++seqNo);
    CheckNodesUpdate(runtime, updates, seqNo, epoch);
    runtime.ClosePipe(pipe, sender, 0);
}

void AsyncResolveNode(TTestActorRuntime &runtime,
                      TActorId sender,
                      ui32 nodeId,
                      TDuration responseTime = TDuration::Max())
{
    TMonotonic deadline = TMonotonic::Max();
    if (responseTime != TDuration::Max()) {
        deadline = runtime.GetCurrentMonotonicTime() + responseTime;
    }
    TAutoPtr<TEvInterconnect::TEvResolveNode> event = new TEvInterconnect::TEvResolveNode(nodeId, deadline);
    runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, event.Release()));
}

void CheckAsyncResolveNode(TTestActorRuntime &runtime, ui32 nodeId, const TString &addr)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvLocalNodeInfo>(handle);
    UNIT_ASSERT(reply);

    UNIT_ASSERT_VALUES_EQUAL(reply->NodeId, nodeId);
    UNIT_ASSERT_VALUES_EQUAL(reply->Addresses[0].GetAddress(), addr);
}

void CheckAsyncResolveUnknownNode(TTestActorRuntime &runtime, ui32 nodeId)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvLocalNodeInfo>(handle);
    UNIT_ASSERT(reply);

    UNIT_ASSERT_VALUES_EQUAL(reply->NodeId, nodeId);
    UNIT_ASSERT(reply->Addresses.empty());
}

void CheckResolveNode(TTestActorRuntime &runtime,
                      TActorId sender,
                      ui32 nodeId,
                      const TString &addr,
                      TDuration responseTime = TDuration::Max())
{
    AsyncResolveNode(runtime, sender, nodeId, responseTime);
    CheckAsyncResolveNode(runtime, nodeId, addr);
}

void CheckResolveUnknownNode(TTestActorRuntime &runtime,
                             TActorId sender,
                             ui32 nodeId,
                             TDuration responseTime = TDuration::Max())
{
    AsyncResolveNode(runtime, sender, nodeId, responseTime);
    CheckAsyncResolveUnknownNode(runtime, nodeId);
}

void CheckNoPendingCacheMissesLeft(TTestActorRuntime &runtime, ui32 nodeIndex)
{
    const auto* nameserver = dynamic_cast<TDynamicNameserver*>(runtime.FindActor(GetNameserviceActorId(), nodeIndex));
    UNIT_ASSERT(nameserver != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(nameserver->GetTotalPendingCacheMissesSize(), 0);
}

THolder<TEvInterconnect::TEvNodesInfo> GetNameserverNodesListEv(TTestActorRuntime &runtime, TActorId sender) {
    runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, new TEvInterconnect::TEvListNodes));

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handle);
    UNIT_ASSERT(reply);
    
    return IEventHandle::Release<TEvInterconnect::TEvNodesInfo>(handle);
}

void GetNameserverNodesList(TTestActorRuntime &runtime,
                            TActorId sender,
                            THashMap<ui32, TEvInterconnect::TNodeInfo> &nodes,
                            bool includeStatic)
{
    ui32 maxStaticNodeId = runtime.GetAppData().DynamicNameserviceConfig->MaxStaticNodeId;
    auto ev = GetNameserverNodesListEv(runtime, sender);

    for (auto &node : ev->Nodes)
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

void RestartNodeBrokerEnsureReadOnly(TTestActorRuntime &runtime)
{
    TBlockEvents<TEvTablet::TEvCommit> block(runtime);
    RestartNodeBroker(runtime);
    block.Unblock();
}

TString HexEscaped(const TString &s) {
    std::ostringstream oss;
    for (unsigned char c : s) {
        oss << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
    }
    return oss.str();
}

class TUpdateNodeLocalDbBuilder {
public:
    TUpdateNodeLocalDbBuilder& SetHost(const TString& host) {
        Host = host;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetPort(ui32 port) {
        Port = port;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetResolveHost(const TString& resolveHost) {
        ResolveHost = resolveHost;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetAddress(const TString& address) {
        Address = address;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetLease(ui32 lease) {
        Lease = lease;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetExpire(ui64 expire) {
        Expire = expire;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetLocation(const TNodeLocation &location) {
        Location = location;;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetServicedSubdomain(const TSubDomainKey& subdomain) {
        ServicedSubdomain = subdomain;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetSlotIndex(ui32 slotIndex) {
        SlotIndex = slotIndex;
        return *this;
    }

    TUpdateNodeLocalDbBuilder& SetAuthorizedByCertificate(bool authorized) {
        AuthorizedByCertificate = authorized;
        return *this;
    }

    TString BuildQuery() const {
        TStringBuilder query;

        if (Host) {
            query << "'('Host (Utf8 '\"" << *Host << "\"))\n";
        }

        if (Port) {
            query << "'('Port (Uint32 '" << *Port << "))\n";
        }

        if (ResolveHost) {
            query << "'('ResolveHost (Utf8 '\"" << *ResolveHost << "\"))\n";
        }

        if (Address) {
            query << "'('Address (Utf8 '\"" << *Address << "\"))\n";
        }

        if (Lease) {
            query << "'('Lease (Uint32 '" << *Lease << "))\n";
        }

        if (Expire) {
            query << "'('Expire (Uint64 '" << *Expire << "))\n";
        }

        if (Location) {
            query << "'('Location (String '\"" << HexEscaped(Location->GetSerializedLocation()) << "\"))\n";
        }

        if (ServicedSubdomain) {
            query << "'('ServicedSubDomain (String '\"" << HexEscaped(ServicedSubdomain->SerializeAsString()) << "\"))\n";
        }

        if (SlotIndex) {
            query << "'('SlotIndex (Uint32 '" << *SlotIndex << "))\n";
        }

        if (AuthorizedByCertificate) {
            query << "'('AuthorizedByCertificate (Bool '" << (*AuthorizedByCertificate ? "true" : "false") << "))\n";
        }

        return query;
    }

    TMaybe<TString> Host;
    TMaybe<ui32> Port;
    TMaybe<TString> ResolveHost;
    TMaybe<TString> Address;
    TMaybe<ui32> Lease;
    TMaybe<ui64> Expire;
    TMaybe<TNodeLocation> Location;
    TMaybe<NKikimrSubDomains::TDomainKey> ServicedSubdomain;
    TMaybe<ui32> SlotIndex;
    TMaybe<bool> AuthorizedByCertificate;
};

void LocalMiniKQL(TTestBasicRuntime& runtime, TActorId sender, ui64 tabletId, const TString& query) {
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetText(query);

    ForwardToTablet(runtime, tabletId, sender, request.Release());

    auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(sender);
    const auto& response = ev->Get()->Record;

    NYql::TIssues programErrors;
    NYql::TIssues paramsErrors;
    NYql::IssuesFromMessage(response.GetCompileResults().GetProgramCompileErrors(), programErrors);
    NYql::IssuesFromMessage(response.GetCompileResults().GetParamsCompileErrors(), paramsErrors);
    TString err = programErrors.ToString() + paramsErrors.ToString() + response.GetMiniKQLErrors();

    UNIT_ASSERT_VALUES_EQUAL(err, "");
    UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NKikimrProto::OK);
}

void UpdateNodesLocalDb(TTestBasicRuntime& runtime, TActorId sender, const THashMap<ui32, TUpdateNodeLocalDbBuilder>& nodes) {
    TStringBuilder query;
    query << "(";
    for (const auto& [id, n] : nodes) {
        query << Sprintf("(let key%u '('('ID (Uint32 '%u))))", id, id);
        query << Sprintf("(let row%u '(%s))", id, n.BuildQuery().data());
    }
    query << "  (return (AsList ";
    for (const auto& [id, _] : nodes) {
        query << Sprintf("(UpdateRow 'Nodes key%u row%u)", id, id);
    }
    query << ")))";
    LocalMiniKQL(runtime, sender, MakeNodeBrokerID(), query);
}

void UpdateNodeLocalDb(TTestBasicRuntime& runtime, TActorId sender, ui32 nodeId, TUpdateNodeLocalDbBuilder& node) {
    UpdateNodesLocalDb(runtime, sender, {{ nodeId, node }});
}

void DeleteNodesLocalDb(TTestBasicRuntime& runtime, TActorId sender, const TVector<ui32> &nodeIds) {
    TStringBuilder query;
    query << "(";
    for (auto id : nodeIds) {
        query << Sprintf("(let key%u '('('ID (Uint32 '%u))))", id, id);
    }
    query << "(return (AsList";
    for (auto id : nodeIds) {
        query << Sprintf("(EraseRow 'Nodes key%u)", id);
    }
    query << ")))";
    LocalMiniKQL(runtime, sender, MakeNodeBrokerID(), query);
}

void DeleteNodeLocalDb(TTestBasicRuntime& runtime, TActorId sender, ui32 nodeId) {
    DeleteNodesLocalDb(runtime, sender, { nodeId });
}

void UpdateParamsLocalDb(TTestBasicRuntime& runtime, TActorId sender, ui32 key, ui64 value) {
    TString query = Sprintf(
        "("
        "  (let key '('('Key (Uint32 '%u))))"
        "  (let row '('('Value (Uint64 '%" PRIu64 "))))"
        "  (return (AsList (UpdateRow 'Params key row)))"
        ")",
        key, value
    );

    LocalMiniKQL(runtime, sender, MakeNodeBrokerID(), query);
}

void UpdateEpochLocalDb(TTestBasicRuntime& runtime, TActorId sender, const NKikimrNodeBroker::TEpoch& newEpoch) {
    UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyCurrentEpochId, newEpoch.GetId());
    UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyCurrentEpochVersion, newEpoch.GetVersion());
    UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyCurrentEpochStart, newEpoch.GetStart());
    UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyCurrentEpochEnd, newEpoch.GetEnd());
    UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyNextEpochEnd, newEpoch.GetNextEnd());
}

NKikimrNodeBroker::TEpoch NextEpochObject(const NKikimrNodeBroker::TEpoch& epoch) {
    auto epochDuration = epoch.GetEnd() - epoch.GetStart();
    NKikimrNodeBroker::TEpoch newEpoch;
    newEpoch.SetId(epoch.GetId() + 1);
    newEpoch.SetVersion(epoch.GetVersion() + 1);
    newEpoch.SetStart(epoch.GetEnd());
    newEpoch.SetEnd(epoch.GetNextEnd());
    newEpoch.SetNextEnd(epoch.GetNextEnd() + epochDuration);
    return newEpoch;
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

    Y_UNIT_TEST(TestListNodesEpochDeltas)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 10);
        TActorId sender = runtime.AllocateEdgeActor();

        WaitForEpochUpdate(runtime, sender);
        WaitForEpochUpdate(runtime, sender);

        auto epoch0 = GetEpoch(runtime, sender);
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch0.GetNextEnd());
        auto epoch1 = CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch0.GetVersion());
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2, epoch1.GetNextEnd());
        auto epoch2 = CheckFilteredNodesList(runtime, sender, {NODE2}, {}, 0, epoch1.GetVersion());
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.6",
                          1, 2, 3, 6, TStatus::OK, NODE3, epoch2.GetNextEnd());
        auto epoch3 = CheckFilteredNodesList(runtime, sender, {NODE3}, {}, 0, epoch2.GetVersion());

        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch0.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE2, NODE3}, {}, 0, epoch1.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch3.GetVersion());

        RebootTablet(runtime, MakeNodeBrokerID(), sender);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch3.GetVersion());

        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.7",
                          1, 2, 3, 7, TStatus::OK, NODE4, epoch3.GetNextEnd());
        auto epoch4 = CheckFilteredNodesList(runtime, sender, {NODE4}, {}, 0, epoch3.GetVersion());

        // NodeBroker persistently stores history
        CheckFilteredNodesList(runtime, sender, {NODE3, NODE4}, {}, 0, epoch2.GetVersion());

        WaitForEpochUpdate(runtime, sender);
        auto epoch5 = GetEpoch(runtime, sender);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch5.GetVersion());

        // New epoch may remove nodes, so deltas are not returned on epoch change
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3, NODE4}, {}, 0, epoch3.GetVersion());
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
                            epoch.SetVersion(epoch.GetVersion() + 1);
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
                            epoch.SetVersion(epoch.GetVersion() + 1);
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
            UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetId(), epoch.GetId());
            UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetStart(), epoch.GetStart());
            UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetEnd(), epoch.GetEnd());
            UNIT_ASSERT_VALUES_EQUAL(rec.GetEpoch().GetNextEnd(), epoch.GetNextEnd());
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

    Y_UNIT_TEST(NoEffectBeforeCommit)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Block commits
        TBlockEvents<TEvTablet::TEvCommit> block(runtime);

        // Register node NODE1
        AsyncRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4);

        // Wait until commit is blocked
        runtime.WaitFor("commit", [&]{ return block.size() >= 1; });

        // No effect before commit
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Unblock commit
        block.Unblock();

        // Check registration request
        CheckAsyncRegistration(runtime, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());

        // Check committed state
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(RegistrationPipelining)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);

        // Block commits
        TBlockEvents<TEvTablet::TEvCommit> block(runtime);

        // Register node NODE1
        AsyncRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4);
        // Register node NODE2
        AsyncRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4");
        // Register node NODE2 with location info
        AsyncRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4);

        // Wait until commits are blocked
        runtime.WaitFor("commit", [&]{ return block.size() >= 1; });

        // No effect before commit
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Unblock commit
        block.Unblock();
        block.Stop();

        // Check registrations requests
        CheckAsyncRegistration(runtime, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());
        CheckAsyncRegistration(runtime, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                               0, 0, 0, 0, TStatus::OK, NODE2, epoch.GetNextEnd());
        CheckAsyncRegistration(runtime, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE2, epoch.GetNextEnd());

        // Check committed state
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(RegistrationPipeliningNodeName)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4, { "/dc-1/my-database", "/dc-1/yet-another-database" });
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE4, TStatus::WRONG_REQUEST);

        // Block commits
        TBlockEvents<TEvTablet::TEvCommit> block(runtime);

        // Register node NODE1
        AsyncRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, false, "/dc-1/my-database");
        // Register node NODE2
        AsyncRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, false, "/dc-1/my-database");
        // Register node NODE2 with different tenant and release its node name
        AsyncRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, false, "/dc-1/yet-another-database");
        // Register node NODE3 and reuse NODE2 node name
        AsyncRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, false, "/dc-1/my-database");
        // Shutdown NODE3 and release its node name
        AsyncGracefulShutdown(runtime, sender, NODE3);
        // Register node NODE4 and reuse NODE3 node name
        AsyncRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, false, "/dc-1/my-database");

        // Wait until commits are blocked
        runtime.WaitFor("commit", [&]{ return block.size() >= 1; });

        // No effect before commit
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE4, TStatus::WRONG_REQUEST);

        // Unblock commit
        block.Unblock();
        block.Stop();

        // Check requests
        CheckAsyncRegistration(runtime, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd(), {}, "slot-0");
        CheckAsyncRegistration(runtime, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE2, epoch.GetNextEnd(), {}, "slot-1");
        CheckAsyncRegistration(runtime, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE2, epoch.GetNextEnd(), {}, "slot-0");
        CheckAsyncRegistration(runtime, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE3, epoch.GetNextEnd(), {}, "slot-1");
        CheckAsyncGracefulShutdown(runtime);
        CheckAsyncRegistration(runtime, "host4", 1001, "host4.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE4, epoch.GetNextEnd(), {}, "slot-1");

        // Check committed state
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3, NODE4}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE3, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE4, "host4", 1001, "host4.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(ConfigPipelining)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        // There should be no dynamic nodes initially
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Block commits
        TBlockEvents<TEvTablet::TEvCommit> block(runtime);

        // Ban all ids
        AsyncSetBannedIds(runtime, sender, {{ NODE1, NODE4 }});

        // Register node
        AsyncRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4);

        // Unban NODE2
        AsyncSetBannedIds(runtime, sender, {{ NODE1, NODE1 }, { NODE3, NODE4 }});

        // Register node again
        AsyncRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4);

        // Wait until commit is blocked
        runtime.WaitFor("commit", [&]{ return block.size() >= 1; });

        // Unblock commit
        block.Unblock();

        // Check registration is failed - no free ids
        CheckAsyncRegistration(runtime, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::ERROR_TEMP);
        // Check registration is successful
        CheckAsyncRegistration(runtime, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE2, epoch.GetNextEnd());

        // Observe registration effect
        CheckNodesList(runtime, sender, {NODE2}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE2, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(UpdateEpochPipelining)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        // Register node NODE1
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // There should NODE1 in the node list
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epochs to expire NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Register new node, should be no free ids
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::ERROR_TEMP);

        // Block commits
        TBlockEvents<TEvTablet::TEvCommit> block(runtime);

        // Move epoch to free NODE1 id
        MoveToNextEpoch(runtime, epoch);
        // Register new node again, should be successful
        AsyncRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4);

        // Wait until commit is blocked
        runtime.WaitFor("commit", [&]{ return block.size() >= 1; });

        // No effect before commit
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Unblock commit
        block.Unblock();
        block.Stop();

        // Check registration request
        epoch = WaitForEpochUpdate(runtime, sender, epoch);
        CheckAsyncRegistration(runtime, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                               1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());

        // Check committed state
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(ExtendLeasePipelining)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        // Register node NODE1
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch without extending lease
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetEnd());

        // Block commits
        TBlockEvents<TEvTablet::TEvCommit> block(runtime);

        // Extend lease and move epoch
        AsyncLeaseExtension(runtime, sender, NODE1);
        MoveToNextEpoch(runtime, epoch);

        // Wait until commit is blocked
        runtime.WaitFor("commit", [&]{ return block.size() >= 1; });

        // Unblock commit
        block.Unblock();
        block.Stop();

        // Check extend lease
        CheckAsyncLeaseExtension(runtime, NODE1, TStatus::OK, epoch);
        epoch = WaitForEpochUpdate(runtime, sender, epoch);

        // Check committed state
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetEnd());
    }

    Y_UNIT_TEST(LoadStateMoveEpoch)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        // Register node NODE1
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // There should NODE1 in the node list
        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move two epochs further to expire NODE1
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetNextEnd() + 1));

        // Restart NodeBroker to trigger loading state
        RestartNodeBroker(runtime);

        // Check epoch
        epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Move epoch to remove NODE1
        runtime.UpdateCurrentTime(TInstant::FromValue(epoch.GetEnd() + 1));

        // Restart NodeBroker to trigger loading state
        RestartNodeBroker(runtime);

        // Check epoch
        epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesAlreadyMigrated)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        // Add new node
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          0, 0, 0, 0, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      0, 0, 0, 0, epoch.GetNextEnd());

        // Restart to trigger Nodes migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Already migrated, so version remains the same
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      0, 0, 0, 0, epoch.GetNextEnd());

        // Reregister with location update
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                        1, 2, 3, 4, epoch.GetNextEnd());

        // Restart to trigger Nodes migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Already migrated, so version remains the same
        epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                        1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to expire NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Restart to trigger Nodes migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Already migrated, so version remains the same
        epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Move epoch to remove NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Restart to trigger Nodes migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Already migrated, so version remains the same
        epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Reuse NodeID by different node
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE1);

        epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                      1, 2, 3, 5, epoch.GetNextEnd());

        // Restart to trigger Nodes migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Already migrated, so version remains the same
        epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                      1, 2, 3, 5, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(NodesMigrationExtendLease)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 2);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate lease extension while NodeBroker is running on the old version
        ui64 extendedExpire = epoch.GetNextEnd() + 1000;
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetLease(2)
                .SetExpire(extendedExpire));
        UpdateNodeLocalDb(runtime, sender, NODE2,
            TUpdateNodeLocalDbBuilder()
                .SetLease(2)
                .SetExpire(extendedExpire));

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // Lease extension is migrated, so version is bumped
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE2}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, extendedExpire);
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, extendedExpire);
    }

    Y_UNIT_TEST(NodesMigrationSetLocation)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          0, 0, 0, 0, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      0, 0, 0, 0, epoch.GetNextEnd());

        // Simulate set location while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
        );

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // Set location is migrated, so version is bumped
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(NodesMigrationNodeName)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 2);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckRegistration(runtime, sender, "host1", 1001, DOMAIN_NAME,
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-0");
        epoch = GetEpoch(runtime, sender);

        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1", "",
                      0, 0, 0, 0, epoch.GetNextEnd());

        // Simulate changing node name while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1, TUpdateNodeLocalDbBuilder().SetSlotIndex(1));

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // Node name change is migrated, but version is not bumped
        // as node name is not included in DynamicNameserver cache
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1", "",
                      0, 0, 0, 0, epoch.GetNextEnd());
        CheckRegistration(runtime, sender, "host1", 1001, DOMAIN_NAME,
                          TStatus::OK, NODE1, epoch.GetNextEnd(), "slot-1");
        CheckRegistration(runtime, sender, "host2", 1001, DOMAIN_NAME,
                          TStatus::OK, NODE2, epoch.GetNextEnd(), "slot-0");
    }

    Y_UNIT_TEST(NodesMigrationExpireActive)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is expired
        auto newEpoch = NextEpochObject(NextEpochObject(epoch));
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 expiration is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {NODE1}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationExpireRemoved)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to remove NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Simulate epoch update while NodeBroker is running on the old version, so new NODE1 is expired
        auto newEpoch = NextEpochObject(NextEpochObject(epoch));
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 expiration is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {NODE1}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationExpiredChanged)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to expire NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Simulate epoch update while NodeBroker is running on the old version, so new NODE1 is removed
        auto newEpoch = NextEpochObject(epoch);
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        newEpoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Simulate epoch update while NodeBroker is running on the old version, so new NODE1 is expired
        newEpoch = NextEpochObject(NextEpochObject(epoch));
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 expiration is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {NODE1}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationRemoveActive)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is removed
        auto newEpoch = NextEpochObject(NextEpochObject(NextEpochObject(epoch)));
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 removal is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationRemoveExpired)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to expire NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is removed
        auto newEpoch = NextEpochObject(epoch);
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 removal is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationRemovedChanged)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to remove NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Simulate epoch update while NodeBroker is running on the old version, so new NODE1 is removed
        auto newEpoch = NextEpochObject(NextEpochObject(NextEpochObject(epoch)));
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 removal is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationReuseID)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is removed
        auto newEpoch = NextEpochObject(NextEpochObject(NextEpochObject(epoch)));
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(newEpoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        newEpoch.SetVersion(newEpoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 reuse is migrated, version was bumped because of possible lease extension
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, newEpoch.GetNextEnd());
    }

    Y_UNIT_TEST(NodesMigrationReuseExpiredID)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to expire NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {NODE1}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is removed
        auto newEpoch = NextEpochObject(NextEpochObject(NextEpochObject(epoch)));
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(newEpoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        newEpoch.SetVersion(newEpoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 reuse is migrated, version was bumped because of possible lease extension
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, newEpoch.GetNextEnd());
    }

    Y_UNIT_TEST(NodesMigrationReuseRemovedID)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to remove NODE1
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 reuse is migrated, version was bumped because of possible lease extension
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(NodesMigrationExtendLeaseThenExpire)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate lease extension while NodeBroker is running on the old version
        auto newEpoch = NextEpochObject(epoch);
        ui64 extendedExpire = newEpoch.GetNextEnd();
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetLease(2)
                .SetExpire(extendedExpire));

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is expired
        newEpoch = NextEpochObject(NextEpochObject(newEpoch));
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 expiration is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {NODE1}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationExtendLeaseThenRemove)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate lease extension while NodeBroker is running on the old version
        auto newEpoch = NextEpochObject(epoch);
        ui64 extendedExpire = newEpoch.GetNextEnd();
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetLease(2)
                .SetExpire(extendedExpire));

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is removed
        newEpoch = NextEpochObject(NextEpochObject(NextEpochObject(newEpoch)));
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 removal is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesMigrationReuseIDThenExtendLease)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is removed
        auto newEpoch = NextEpochObject(NextEpochObject(NextEpochObject(epoch)));
        UpdateEpochLocalDb(runtime, sender, newEpoch);
        DeleteNodeLocalDb(runtime, sender, NODE1);

        // Register new node with the same ID while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host2")
                .SetPort(1001)
                .SetResolveHost("host2.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(newEpoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        newEpoch.SetVersion(newEpoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Simulate lease extension while NodeBroker is running on the old version
        ui64 extendedExpire = newEpoch.GetNextEnd() + 1000;
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetLease(2)
                .SetExpire(extendedExpire));

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // NODE1 reuse is migrated, version was bumped because of lease extension
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, extendedExpire);
    }

    Y_UNIT_TEST(NodesMigrationNewActiveNode)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());

        // Register new nodes while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host1")
                .SetPort(1001)
                .SetResolveHost("host1.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // New NODE1 is migrated, version was bumped because of possible lease extension
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
    }

    Y_UNIT_TEST(NodesMigrationNewExpiredNode)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());

        // Register new nodes while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE1,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host1")
                .SetPort(1001)
                .SetResolveHost("host1.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(0)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Simulate epoch update while NodeBroker is running on the old version, so NODE1 is expired
        auto newEpoch = NextEpochObject(NextEpochObject(epoch));
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        // New expired NODE1 reuse is migrated, version was bumped only during epoch change
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(newEpoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {}, {NODE1}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, newEpoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, newEpoch.GetVersion(), epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ShiftIdRangeRemoveActive)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, epoch.GetId());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend leases
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);

        epoch = GetEpoch(runtime, sender);

        // Shift ID range to [NODE1, NODE2]
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE2;

        // Restart to trigger node removal due to shift ID range
        RestartNodeBroker(runtime);

        // Check that node removal bump version
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE2}, {}, 0, epochAfterRestart.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2}, {}, 0, epochAfterRestart.GetVersion() - 4);
        CheckUpdateNodesLog(runtime, sender, {}, epochAfterRestart.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 2, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 3, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 4, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ShiftIdRangeRemoveExpired)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, epoch.GetId());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend leases
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        // Move epoch so NODE3 is expired
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {NODE3}, epoch.GetId());

        // Extend leases again
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = GetEpoch(runtime, sender);

        // Shift ID range to [NODE1, NODE2]
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE2;

        // Restart to trigger node removal due to shift ID range
        RestartNodeBroker(runtime);

        // Check that node removal bump version
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE2}, {}, 0, epochAfterRestart.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2}, {}, 0, epochAfterRestart.GetVersion() - 3);
        CheckUpdateNodesLog(runtime, sender, {}, epochAfterRestart.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 2, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 3, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ShiftIdRangeRemoveReusedID)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, epoch.GetId());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend leases
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend leases again
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        // Move epoch so NODE3 is removed
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epoch.GetId());

        // Extend leases again
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = GetEpoch(runtime, sender);

        // Register new node while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE3,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host3")
                .SetPort(1001)
                .SetResolveHost("host3.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(2)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Shift ID range to [NODE1, NODE2]
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE2;

        // Restart to trigger node removal due to shift ID range
        RestartNodeBroker(runtime);

        // Check that node removal bump version
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE2}, {}, 0, epochAfterRestart.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2}, {}, 0, epochAfterRestart.GetVersion() - 4);
        CheckUpdateNodesLog(runtime, sender, {}, epochAfterRestart.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 2, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 3, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 4, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ShiftIdRangeRemoveNew)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epoch.GetId());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend leases
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = GetEpoch(runtime, sender);

        // Register new node while NodeBroker is running on the old version
        UpdateNodeLocalDb(runtime, sender, NODE3,
            TUpdateNodeLocalDbBuilder()
                .SetHost("host3")
                .SetPort(1001)
                .SetResolveHost("host3.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(2)
        );
        epoch.SetVersion(epoch.GetVersion() + 1);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Shift ID range to [NODE1, NODE2]
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE2;

        // Restart to trigger node removal due to shift ID range
        RestartNodeBroker(runtime);

        // Check that node removal bump version
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE2}, {}, 0, epochAfterRestart.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2}, {}, 0, epochAfterRestart.GetVersion() - 4);
        CheckUpdateNodesLog(runtime, sender, {}, epochAfterRestart.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE3)}, epochAfterRestart.GetVersion() - 2, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 3, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE2, R(NODE3)}, epochAfterRestart.GetVersion() - 4, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ExtendLeaseBumpVersion)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1,}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Move epoch to be able to extend lease
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend lease
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        // Check that extend lease bumps epoch version
        auto epochAfterExtendLease = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterExtendLease.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion(), epochAfterExtendLease);

        // Extend lease one more time without moving epoch
        epoch = epochAfterExtendLease;
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        // Check that extend lease without moving epoch doesn't bump epoch version
        epochAfterExtendLease = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterExtendLease.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterExtendLease);
    }

    Y_UNIT_TEST(ListNodesEpochDeltasPersistance)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        // Register new nodes
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          0, 0, 0, 0, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        // Update existing nodes
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // Check deltas
        auto epoch = GetEpoch(runtime, sender);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE3, NODE1}, {}, 0, epoch.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE2, NODE3, NODE1}, {}, 0, epoch.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3, NODE1}, {}, 0, epoch.GetVersion() - 4);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE1}, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1}, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE2, NODE3, NODE1}, epoch.GetVersion() - 4, epoch);

        // Restart NodeBroker
        RestartNodeBroker(runtime);

        // Deltas are preserved after NodeBroker restart, but compacted
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE3, NODE1}, {}, 0, epoch.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE2, NODE3, NODE1}, {}, 0, epoch.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE2, NODE3, NODE1}, {}, 0, epoch.GetVersion() - 4);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE1}, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1}, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1}, epoch.GetVersion() - 4, epoch);

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Old deltas live only until the epoch end
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 4);

        // New deltas are preserved after epoch end, but compacted
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE1}, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1}, epoch.GetVersion() - 4, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1}, epoch.GetVersion() - 5, epoch);

        // Extend lease
        CheckLeaseExtension(runtime, sender, NODE3, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        // Check deltas
        epoch = GetEpoch(runtime, sender);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE2, NODE1}, {}, 0, epoch.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE3, NODE2, NODE1}, {}, 0, epoch.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3, NODE3, NODE2, NODE1}, {}, 0, epoch.GetVersion() - 4);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE1}, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 4, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1, NODE3, NODE2, NODE1}, epoch.GetVersion() - 5, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE1, NODE3, NODE2, NODE1}, epoch.GetVersion() - 6, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1, NODE3, NODE2, NODE1}, epoch.GetVersion() - 7, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE3, NODE1, NODE3, NODE2, NODE1}, epoch.GetVersion() - 8, epoch);

        // Simulate epoch update while NodeBroker is running on the old version
        auto newEpoch = NextEpochObject(epoch);
        UpdateEpochLocalDb(runtime, sender, newEpoch);

        // Restart NodeBroker
        RestartNodeBroker(runtime);

        // Old deltas live only until the epoch end
        epoch = GetEpoch(runtime, sender);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 3);
        CheckFilteredNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, 0, epoch.GetVersion() - 4);

        // New deltas are preserved after epoch end, but compacted
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE2, NODE1}, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 4, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 5, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 6, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 7, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 8, epoch);
        CheckUpdateNodesLog(runtime, sender, {NODE3, NODE2, NODE1}, epoch.GetVersion() - 9, epoch);
    }

    Y_UNIT_TEST(ExtendLeaseSetLocationInOneRegistration)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 1);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          0, 0, 0, 0, TStatus::OK, NODE1, epoch.GetNextEnd());

        CheckNodesList(runtime, sender, {NODE1,}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      0, 0, 0, 0, epoch.GetNextEnd());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Extend lease and set location in one registration
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());

        CheckNodesList(runtime, sender, {NODE1,}, {}, epoch.GetId());
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());

        // Check that both updates happen with one version bump
        auto epochAfterRegistration = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRegistration.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion());
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion(), epochAfterRegistration);
    }

    Y_UNIT_TEST(EpochCacheUpdate)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 2);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Get epoch nodes full list
        auto nodes = ListNodes(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes(0).GetNodeId(), NODE1);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes(0).GetExpire(), epoch.GetEnd());

        // Update one node
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        // Make sure update is visible in epoch nodes full list
        nodes = ListNodes(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes(1).GetNodeId(), NODE1);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes(1).GetExpire(), epoch.GetNextEnd());

        // Move epoch and update node again
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        // Register new node
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2, epoch.GetNextEnd());

        // Make sure both nodes are visible in epoch nodes full list
        nodes = ListNodes(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetNodes().size(), 3);
        TVector<ui32> actualIds;
        for (const auto& node : nodes.GetNodes()) {
            actualIds.push_back(node.GetNodeId());
        }
        TVector<ui32> expectedIds = {NODE1, NODE1, NODE2};
        UNIT_ASSERT_VALUES_EQUAL(actualIds, expectedIds);
    }

    Y_UNIT_TEST(NodesV2BackMigration)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, epoch.GetId());

        // Move epoch in a such way that NODE2 is expired and NODE3 is removed
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        epoch = GetEpoch(runtime, sender);

        CheckNodesList(runtime, sender, {NODE1}, {NODE2}, epoch.GetId());

        // Clean data in Nodes table
        DeleteNodeLocalDb(runtime, sender, NODE1);
        DeleteNodeLocalDb(runtime, sender, NODE2);

        // Set NodesV2 as main table
        UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyMainNodesTable, static_cast<ui64>(Schema::EMainNodesTable::NodesV2));

        // Restart to trigger nodes back migration
        RestartNodeBroker(runtime);

        // Check migration
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {NODE2}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE1}, {NODE2}, 0, epoch.GetVersion() - 2);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {E(NODE2), R(NODE3), NODE1}, epoch.GetVersion() - 2, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);

        // Restart one more time, there should be no migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Check migration again
        epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {NODE2}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epoch.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {NODE1}, {NODE2}, 0, epoch.GetVersion() - 2);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1}, epoch.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {E(NODE2), R(NODE3), NODE1}, epoch.GetVersion() - 2, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                        1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(NodesV2BackMigrationShiftIdRange)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, epoch.GetId());

        // Move epoch in a such way that NODE3 is removed
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);
        epoch = GetEpoch(runtime, sender);

        CheckNodesList(runtime, sender, {NODE1, NODE2}, {}, epoch.GetId());

        // Clean data in Nodes table
        DeleteNodeLocalDb(runtime, sender, NODE1);
        DeleteNodeLocalDb(runtime, sender, NODE2);

        // Set NodesV2 as main table
        UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyMainNodesTable, static_cast<ui64>(Schema::EMainNodesTable::NodesV2));

        // Shift ID range to [NODE1, NODE1]
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE1;

        // Restart to trigger nodes back migration with shift range ID
        RestartNodeBroker(runtime);

        // Check migration with shift range ID
        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epochAfterRestart.GetVersion() - 3);
        CheckUpdateNodesLog(runtime, sender, {}, epochAfterRestart.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE2)}, epochAfterRestart.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE2)}, epochAfterRestart.GetVersion() - 2, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1, R(NODE2)}, epochAfterRestart.GetVersion() - 3, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);

        // Restart one more time, there should be no migration
        RestartNodeBrokerEnsureReadOnly(runtime);

        // Check migration with shift range ID again
        epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, {NODE1}, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 1);
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epochAfterRestart.GetVersion() - 2);
        CheckFilteredNodesList(runtime, sender, {NODE1}, {}, 0, epochAfterRestart.GetVersion() - 3);
        CheckUpdateNodesLog(runtime, sender, {}, epochAfterRestart.GetVersion(), epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE2)}, epochAfterRestart.GetVersion() - 1, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {R(NODE2)}, epochAfterRestart.GetVersion() - 2, epochAfterRestart);
        CheckUpdateNodesLog(runtime, sender, {NODE1, R(NODE2)}, epochAfterRestart.GetVersion() - 3, epochAfterRestart);
        CheckNodeInfo(runtime, sender, NODE1, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                      1, 2, 3, 4, epoch.GetNextEnd());
        CheckNodeInfo(runtime, sender, NODE2, TStatus::WRONG_REQUEST);
        CheckNodeInfo(runtime, sender, NODE3, TStatus::WRONG_REQUEST);
    }

    void NodesMigrationNNodes(size_t dynamicNodesCount) {
        TTestBasicRuntime runtime(8, false);

        Setup(runtime, dynamicNodesCount);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());

        // Register nodes that going to be expired
        TSet<ui64> expiredNodesIds;
        for (size_t i = 0; i < dynamicNodesCount / 2; ++i)  {
            AsyncRegistration(runtime, sender, "host", 1001 + i, "host.yandex.net", "1.2.3.4",
                              1, 2, 3, 4);
            expiredNodesIds.insert(NODE1 + i);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
        CheckNodesList(runtime, sender, expiredNodesIds, {}, epoch.GetId());

        // Simulate epoch update while NodeBroker is running on the old version
        epoch = NextEpochObject(NextEpochObject(epoch));
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Register new nodes while NodeBroker is running on the old version
        TSet<ui64> activeNodeIds;
        THashMap<ui32, TUpdateNodeLocalDbBuilder> activeNodes;
        for (size_t i = dynamicNodesCount / 2; i < dynamicNodesCount; ++i)  {
            auto node = TUpdateNodeLocalDbBuilder()
                .SetHost("host")
                .SetPort(1001 + i)
                .SetResolveHost("host.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(i);
            activeNodes[NODE1 + i] = node;
            epoch.SetVersion(epoch.GetVersion() + 1);
            activeNodeIds.insert(NODE1 + i);
        }
        UpdateNodesLocalDb(runtime, sender, activeNodes);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Restart to trigger Nodes migration
        RestartNodeBroker(runtime);

        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 1, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, activeNodeIds, expiredNodesIds, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {activeNodeIds.begin(), activeNodeIds.end()}, {}, 0, epoch.GetVersion());
    }

    Y_UNIT_TEST(NodesMigration999Nodes)
    {
        NodesMigrationNNodes(999);
    }

    Y_UNIT_TEST(NodesMigration1000Nodes)
    {
        NodesMigrationNNodes(1000);
    }

    Y_UNIT_TEST(NodesMigration1001Nodes)
    {
        NodesMigrationNNodes(1001);
    }

    Y_UNIT_TEST(NodesMigration2000Nodes)
    {
        NodesMigrationNNodes(2000);
    }

    Y_UNIT_TEST(NodesMigrationManyNodesInterrupted)
    {
        TTestBasicRuntime runtime(8, false);

        constexpr size_t DYNAMIC_NODES_COUNT = 1500;

        Setup(runtime, DYNAMIC_NODES_COUNT);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());

        // Register nodes that going to be expired
        TSet<ui64> expiredNodesIds;
        for (size_t i = 0; i < DYNAMIC_NODES_COUNT / 2; ++i)  {
            AsyncRegistration(runtime, sender, "host", 1001 + i, "host.yandex.net", "1.2.3.4",
                              1, 2, 3, 4);
            expiredNodesIds.insert(NODE1 + i);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
        CheckNodesList(runtime, sender, expiredNodesIds, {}, epoch.GetId());

        // Simulate epoch update while NodeBroker is running on the old version
        epoch = NextEpochObject(NextEpochObject(epoch));
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Register new nodes while NodeBroker is running on the old version
        TSet<ui64> activeNodeIds;
        THashMap<ui32, TUpdateNodeLocalDbBuilder> activeNodes;
        for (size_t i = DYNAMIC_NODES_COUNT / 2; i < DYNAMIC_NODES_COUNT; ++i)  {
            auto node = TUpdateNodeLocalDbBuilder()
                .SetHost("host")
                .SetPort(1001 + i)
                .SetResolveHost("host.yandex.net")
                .SetAddress("1.2.3.4")
                .SetLocation(TNodeLocation("1", "2", "3", "4"))
                .SetLease(1)
                .SetExpire(epoch.GetNextEnd())
                .SetServicedSubdomain(TSubDomainKey(TTestTxConfig::SchemeShard, 1))
                .SetSlotIndex(i);
            activeNodes[NODE1 + i] = node;
            epoch.SetVersion(epoch.GetVersion() + 1);
            activeNodeIds.insert(NODE1 + i);
        }
        UpdateNodesLocalDb(runtime, sender, activeNodes);
        UpdateEpochLocalDb(runtime, sender, epoch);

        // Block commit result to restart during migration
        TBlockEvents<TEvTablet::TEvCommitResult> block(runtime);

        // Restart to trigger Nodes migration
        runtime.Register(CreateTabletKiller(MakeNodeBrokerID()));

        // Restart after first batch is committed
        runtime.WaitFor("first batch is committed", [&]{ return block.size() >= 2; });
        block.Stop();
        RestartNodeBroker(runtime);

        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion() + 2, epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, activeNodeIds, expiredNodesIds, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {activeNodeIds.begin(), activeNodeIds.end()}, {}, 0, epoch.GetVersion());
    }

    Y_UNIT_TEST(NodesV2BackMigrationManyNodesInterrupted)
    {
        TTestBasicRuntime runtime(8, false);

        constexpr size_t DYNAMIC_NODES_COUNT = 1500;

        Setup(runtime, DYNAMIC_NODES_COUNT);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {}, {}, epoch.GetId());

        // Register nodes
        TSet<ui64> activeNodeIds;
        for (size_t i = 0; i < DYNAMIC_NODES_COUNT; ++i)  {
            AsyncRegistration(runtime, sender, "host", 1001 + i, "host.yandex.net", "1.2.3.4",
                              1, 2, 3, 4);
            activeNodeIds.insert(NODE1 + i);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
        CheckNodesList(runtime, sender, activeNodeIds, {}, epoch.GetId());

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);

        // Clean Nodes table
        DeleteNodesLocalDb(runtime, sender, {activeNodeIds.begin(), activeNodeIds.end()});

        // Set NodesV2 as main table
        UpdateParamsLocalDb(runtime, sender, Schema::ParamKeyMainNodesTable, static_cast<ui64>(Schema::EMainNodesTable::NodesV2));

        // Block commit result to restart during migration
        TBlockEvents<TEvTablet::TEvCommitResult> block(runtime);

        // Restart to trigger Nodes back migration
        runtime.Register(CreateTabletKiller(MakeNodeBrokerID()));

        // Restart after first batch is committed
        runtime.WaitFor("first batch is committed", [&]{ return block.size() >= 2; });
        block.Stop();
        RestartNodeBroker(runtime);

        auto epochAfterRestart = GetEpoch(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetVersion(), epochAfterRestart.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(epoch.GetId(), epochAfterRestart.GetId());
        CheckNodesList(runtime, sender, activeNodeIds, {}, epochAfterRestart.GetId());
        CheckFilteredNodesList(runtime, sender, {}, {}, 0, epoch.GetVersion());
    }

    Y_UNIT_TEST(UpdateNodesLog)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        auto epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE2, NODE3}, {}, epoch.GetId());

        // Move epoch in a such way that NODE2 is expired and NODE3 is removed
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);

        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1}, {NODE2}, epoch.GetId());

        // Check sync nodes
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, { E(NODE2), R(NODE3) }, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, E(NODE2), R(NODE3) }, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, E(NODE2), R(NODE3) }, 0, epoch);

        RestartNodeBrokerEnsureReadOnly(runtime);

        // Log is stored persistently
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, { E(NODE2), R(NODE3) }, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, E(NODE2), R(NODE3) }, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, E(NODE2), R(NODE3) }, 0, epoch);

        // Lease extension update node
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        // Register node
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);

        epoch = GetEpoch(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE3}, {NODE2}, epoch.GetId());

        // Check sync nodes
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE3 }, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, NODE3 }, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, { E(NODE2), R(NODE3), NODE1, NODE3 }, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, E(NODE2), R(NODE3), NODE1, NODE3 }, epoch.GetVersion() - 4, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, E(NODE2), R(NODE3), NODE1, NODE3 }, 0, epoch);

        // Move epoch
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesList(runtime, sender, {NODE1, NODE3}, {}, epoch.GetId());

        // Check sync nodes
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, { R(NODE2) }, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE3, R(NODE2) }, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, NODE3, R(NODE2) }, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, NODE3, R(NODE2) }, 0, epoch);

        // Shift ID range to [NODE1, NODE1]
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE1;

        // Restart to trigger shift range ID
        RestartNodeBroker(runtime);
        epoch = GetEpoch(runtime, sender);

        // Check sync nodes
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion(), epoch);
        CheckUpdateNodesLog(runtime, sender, { R(NODE3) }, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, { R(NODE2), R(NODE3) }, epoch.GetVersion() - 2, epoch);
        CheckUpdateNodesLog(runtime, sender, { R(NODE2), R(NODE3) }, epoch.GetVersion() - 3, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, R(NODE2), R(NODE3) }, epoch.GetVersion() - 4, epoch);
        CheckUpdateNodesLog(runtime, sender, { NODE1, R(NODE2), R(NODE3) }, 0, epoch);
    }

    Y_UNIT_TEST(UpdateNodesLogEmptyEpoch)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        // Check initial epoch without nodes
        auto epoch = GetEpoch(runtime, sender);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {}, 0, epoch);

        // Check move epoch without nodes
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckUpdateNodesLog(runtime, sender, {}, epoch.GetVersion() - 1, epoch);
        CheckUpdateNodesLog(runtime, sender, {}, 0, epoch);
    }

    Y_UNIT_TEST(SyncNodes)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 2);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // Subscribe to node updates and check initial update
        TActorId pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
        ui64 cachedVersion = 0;
        ui64 seqNo = 1;
        SubscribeToNodesUpdates(runtime, sender, pipe, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));

        // Make one another update
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        // Sync this update
        CheckSyncNodes(runtime, sender, { NODE2 }, seqNo, pipe, GetEpoch(runtime, sender));

        // Sync one more time without updates
        CheckSyncNodes(runtime, sender, {}, seqNo, pipe, GetEpoch(runtime, sender));
    }

    Y_UNIT_TEST(SubscribeToNodes)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();
        TActorId pipe1 = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());

        // Subscribe to node updates and check initial update
        ui64 cachedVersion = 0;
        ui64 seqNo = 0;
        SubscribeToNodesUpdates(runtime, sender, pipe1, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, {}, seqNo, GetEpoch(runtime, sender));

        // Check updates after registration
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));

        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckNodesUpdate(runtime, { NODE2 }, seqNo, GetEpoch(runtime, sender));

        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);
        CheckNodesUpdate(runtime, { NODE3 }, seqNo, GetEpoch(runtime, sender));

        // Check update after epoch change
        auto epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesUpdate(runtime, {}, seqNo, GetEpoch(runtime, sender));

        // Check update after lease extension
        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));

        CheckLeaseExtension(runtime, sender, NODE2, TStatus::OK, epoch);
        CheckNodesUpdate(runtime, { NODE2 }, seqNo, GetEpoch(runtime, sender));

        // Check update after epoch change that expires NODE3
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesUpdate(runtime, { E(NODE3) }, seqNo, GetEpoch(runtime, sender));
        CheckNodesList(runtime, sender, {NODE1, NODE2}, {NODE3}, epoch.GetId());

        CheckLeaseExtension(runtime, sender, NODE1, TStatus::OK, epoch);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));

        // Connect new client and check initial update
        ui64 cachedVersion2 = 0;
        ui64 seqNo2 = 0;
        TActorId sender2 = runtime.AllocateEdgeActor();
        auto pipe2 = runtime.ConnectToPipe(MakeNodeBrokerID(), sender2, 0, GetPipeConfigWithRetries());
        SubscribeToNodesUpdates(runtime, sender2, pipe2, cachedVersion2, seqNo2);
        CheckNodesUpdate(runtime, { NODE1, NODE2, E(NODE3), NODE1 }, seqNo2, GetEpoch(runtime, sender2));

        // Check update after epoch change that expires NODE2 and removes NODE3
        epoch = WaitForEpochUpdate(runtime, sender);
        CheckNodesUpdate(runtime, { E(NODE2), R(NODE3) }, seqNo, GetEpoch(runtime, sender));
        CheckNodesUpdate(runtime, { E(NODE2), R(NODE3) }, seqNo2, GetEpoch(runtime, sender));
        CheckNodesList(runtime, sender, {NODE1}, {NODE2}, epoch.GetId());
        cachedVersion = epoch.GetVersion();
        cachedVersion2 = epoch.GetVersion();

        // Shift ID range
        auto dnConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dnConfig->MinDynamicNodeId = NODE1;
        dnConfig->MaxDynamicNodeId = NODE1;

        // Restart to trigger shift range ID
        RestartNodeBroker(runtime);

        // Reconnect and check update after shift range ID
        pipe1 = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
        ++seqNo;
        SubscribeToNodesUpdates(runtime, sender, pipe1, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, { R(NODE2) }, seqNo, GetEpoch(runtime, sender));

        // Connect new client and check initial update
        ui64 cachedVersion3 = 0;
        ui64 seqNo3 = 0;
        TActorId sender3 = runtime.AllocateEdgeActor();
        auto pipe3 = runtime.ConnectToPipe(MakeNodeBrokerID(), sender3, 0, GetPipeConfigWithRetries());
        SubscribeToNodesUpdates(runtime, sender3, pipe3, cachedVersion3, seqNo3);
        CheckNodesUpdate(runtime, { NODE1, R(NODE3), R(NODE2) }, seqNo3, GetEpoch(runtime, sender2));
    }

    Y_UNIT_TEST(NodesSubscriberDisconnect)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // Subscribe to node updates and check initial update
        TActorId pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
        ui64 cachedVersion = 0;
        ui64 seqNo = 1;
        SubscribeToNodesUpdates(runtime, sender, pipe, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));

        // Check updates after registration
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);
        CheckNodesUpdate(runtime, { NODE2 }, seqNo, GetEpoch(runtime, sender));

        // Check empty sync nodes
        CheckSyncNodes(runtime, sender, {}, seqNo, pipe, GetEpoch(runtime, sender));

        // Update cached version
        cachedVersion = GetEpoch(runtime, sender).GetVersion();

        // Close pipe
        runtime.ClosePipe(pipe, sender, 0);

        // Check no updates were sent
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> block(runtime);
        CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE3);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(block.empty(), "updates were sent");

        // Resubcribe and check updates
        block.Stop();
        pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
        ++seqNo;
        SubscribeToNodesUpdates(runtime, sender, pipe, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, { NODE3 }, seqNo, GetEpoch(runtime, sender));

        CheckRegistration(runtime, sender, "host4", 1001, "host4.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE4);
        CheckNodesUpdate(runtime, { NODE4 }, seqNo, GetEpoch(runtime, sender));

        // Check empty sync nodes
        CheckSyncNodes(runtime, sender, {}, seqNo, pipe, GetEpoch(runtime, sender));
    }

    Y_UNIT_TEST(SeveralNodesSubscribersPerPipe)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // Subscribe to node updates and check initial update
        TActorId pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
        ui64 cachedVersion = 0;
        ui64 seqNo = 1;
        SubscribeToNodesUpdates(runtime, sender, pipe, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));

        // Subscribe another client to node updates using the same pipe and check initial update
        TActorId sender2 = runtime.AllocateEdgeActor();
        ui64 cachedVersion2 = 0;
        ui64 seqNo2 = 1;
        SubscribeToNodesUpdates(runtime, sender2, pipe, cachedVersion2, seqNo2);
        CheckNodesUpdate(runtime, { NODE1 }, seqNo2, GetEpoch(runtime, sender));

        // Check empty sync nodes
        CheckSyncNodes(runtime, sender, {}, seqNo, pipe, GetEpoch(runtime, sender));
        CheckSyncNodes(runtime, sender2, {}, seqNo2, pipe, GetEpoch(runtime, sender));

        // Delay updates
        {
            TBlockEvents<TEvNodeBroker::TEvUpdateNodes> block(runtime);
            CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                              1, 2, 3, 4, TStatus::OK, NODE2);
            runtime.WaitFor("updates are sent", [&]{ return block.size() >= 2; });
            block.Unblock();
        }

        // Check sync nodes
        CheckSyncNodes(runtime, sender, { NODE2 }, seqNo, pipe, GetEpoch(runtime, sender));
        CheckSyncNodes(runtime, sender2, { NODE2 }, seqNo2, pipe, GetEpoch(runtime, sender));

        // Update cached version
        cachedVersion = GetEpoch(runtime, sender).GetVersion();
        cachedVersion2 = GetEpoch(runtime, sender).GetVersion();

        // Close pipe
        runtime.ClosePipe(pipe, sender, 0);

        // Check no updates were sent
        {
            TBlockEvents<TEvNodeBroker::TEvUpdateNodes> block(runtime);
            CheckRegistration(runtime, sender, "host3", 1001, "host3.yandex.net", "1.2.3.4",
                              1, 2, 3, 4, TStatus::OK, NODE3);
            runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT_C(block.empty(), "updates were sent");
        }

        // Resubcribe only one client and check updates
        pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());
        ++seqNo;
        SubscribeToNodesUpdates(runtime, sender, pipe, cachedVersion, seqNo);
        CheckNodesUpdate(runtime, { NODE3 }, seqNo, GetEpoch(runtime, sender));
    }

    void TestNNodesSubscribers(size_t n)
    {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 3);
        TActorId sender = runtime.AllocateEdgeActor();

        ui64 cachedVersion = 0;
        ui64 seqNo = 0;

        // Subscribe to nodes update and check initial update
        for (size_t i = 0; i < n; ++i) {
            TActorId sender = runtime.AllocateEdgeActor();
            TActorId pipe = runtime.ConnectToPipe(MakeNodeBrokerID(), sender, 0, GetPipeConfigWithRetries());

            SubscribeToNodesUpdates(runtime, sender, pipe, cachedVersion, seqNo);
            CheckNodesUpdate(runtime, {}, seqNo, GetEpoch(runtime, sender));
        }

        // Register node
        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // Check updates after registration
        for (size_t i = 0; i < n; ++i) {
            CheckNodesUpdate(runtime, { NODE1 }, seqNo, GetEpoch(runtime, sender));
        }

        // Register node
        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE2);

        // Check updates after registration
        for (size_t i = 0; i < n; ++i) {
            CheckNodesUpdate(runtime, { NODE2 }, seqNo, GetEpoch(runtime, sender));
        }
    }

    Y_UNIT_TEST(Test999NodesSubscribers)
    {
        TestNNodesSubscribers(999);
    }

    Y_UNIT_TEST(Test1000NodesSubscribers)
    {
        TestNNodesSubscribers(1000);
    }

    Y_UNIT_TEST(Test1001NodesSubscribers)
    {
        TestNNodesSubscribers(1001);
    }
}

Y_UNIT_TEST_SUITE(TDynamicNameserverTest) {
    Y_UNIT_TEST_FLAG(BasicFunctionality, EnableNodeBrokerDeltaProtocol)
    {
        TTestBasicRuntime runtime(8, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
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

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST(TestCacheUsage)
    {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        TVector<NKikimrNodeBroker::TListNodes> listRequests;
        TVector<NKikimrNodeBroker::TResolveNode> resolveRequests;

        auto logRequests = [&](TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvInterconnect::EvListNodes:
                    if (runtime.FindActorName(event->Sender) == "TABLET_RESOLVER_ACTOR") {
                        // block TEvListNodes from tablet resolver
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;

                case TEvNodeBroker::EvListNodes:
                    listRequests.push_back(event->Get<TEvNodeBroker::TEvListNodes>()->Record);
                    break;

                case TEvNodeBroker::EvResolveNode:
                    resolveRequests.push_back(event->Get<TEvNodeBroker::TEvResolveNode>()->Record);
                    break;

                default:
                    break;
            }
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

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST_FLAG(ListNodesCacheWhenNoChanges, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();
        
        // Add one dynamic node in addition to one static node
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
                    1, 2, 3, 4, TStatus::OK, NODE1);

        // Make ListNodes requests that are not batched
        auto ev1 = GetNameserverNodesListEv(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(ev1->Nodes.size(), 2);

        auto ev2 = GetNameserverNodesListEv(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(ev2->Nodes.size(), 2);

        // No changes, so ListNodesCache must be the same
        UNIT_ASSERT_VALUES_EQUAL(ev1->NodesPtr.Get(), ev2->NodesPtr.Get());

        // Add new dynamic node
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
                    1, 2, 3, 5, TStatus::OK, NODE2);

        // Make one more ListNodes request
        auto ev3 = GetNameserverNodesListEv(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(ev3->Nodes.size(), 3);

        // When changes are made, a new ListNodesCache is allocated
        UNIT_ASSERT_VALUES_UNEQUAL(ev2->NodesPtr.Get(), ev3->NodesPtr.Get());
    }

    Y_UNIT_TEST_FLAG(CacheMissPipeDisconnect, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();

        // Block cache miss requests in old protocol
        TBlockEvents<TEvNodeBroker::TEvResolveNode> resolveBlock(runtime);
        // Block cache miss requests in new detlta protocol
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> deltaBlock(runtime);
        TBlockEvents<TEvNodeBroker::TEvSyncNodesRequest> syncBlock(runtime);

        // Register a dynamic node in NodeBroker
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1);

        // Send an asynchronous node resolve request to DynamicNameserver
        AsyncResolveNode(runtime, sender, NODE1);

        // Wait until cache miss is blocked
        runtime.WaitFor("cache miss", [&]{return resolveBlock.size() >= 1 || syncBlock.size() >= 1; });

        // Reboot NodeBroker to break pipe
        RebootTablet(runtime, MakeNodeBrokerID(), sender);

        // Resolve request is failed, because pipe was broken
        CheckAsyncResolveUnknownNode(runtime, NODE1);

        // Stop blocking new cache miss requests
        resolveBlock.Stop();
        syncBlock.Stop();
        deltaBlock.Stop();

        // The following requests should be OK
        CheckResolveNode(runtime, sender, NODE1, "1.2.3.4");

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST_FLAG(CacheMissSimpleDeadline, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();

        // Block cache miss requests in old protocol
        TBlockEvents<TEvNodeBroker::TEvResolveNode> resolveBlock(runtime);
        // Block cache miss requests in new detlta protocol
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> deltaBlock(runtime);
        TBlockEvents<TEvNodeBroker::TEvSyncNodesRequest> syncBlock(runtime);

        // Register dynamic node in NodeBroker
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
            1, 2, 3, 4, TStatus::OK, NODE1);


        // Send an asynchronous node resolve request to DynamicNameserver with deadline
        AsyncResolveNode(runtime, sender, NODE1, TDuration::Seconds(1));

        // Wait until cache miss is blocked
        runtime.WaitFor("cache miss", [&]{ return resolveBlock.size() >= 1 || syncBlock.size() >= 1; });

        // Move time to the deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve request is failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE1);

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST_FLAG(CacheMissSameDeadline, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();

        // Block cache miss requests in old protocol
        TBlockEvents<TEvNodeBroker::TEvResolveNode> resolveBlock(runtime);
        // Block cache miss requests in new detlta protocol
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> deltaBlock(runtime);
        TBlockEvents<TEvNodeBroker::TEvSyncNodesRequest> syncBlock(runtime);

        // Register dynamic nodes in NodeBroker
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
            1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
            1, 2, 3, 5, TStatus::OK, NODE2);

        // Send two asynchronous node resolve requests to DynamicNameserver with same deadline
        AsyncResolveNode(runtime, sender, NODE1, TDuration::Seconds(1));
        AsyncResolveNode(runtime, sender, NODE2, TDuration::Seconds(1));

        // Wait until cache misses are blocked
        runtime.WaitFor("cache miss", [&]{ return resolveBlock.size() >= 2 || syncBlock.size() >= 1; });

        // Move time to the deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve requests are failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE1);
        CheckAsyncResolveUnknownNode(runtime, NODE2);

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST_FLAG(CacheMissNoDeadline, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();

        // Block cache miss requests in old protocol
        TBlockEvents<TEvNodeBroker::TEvResolveNode> resolveBlock(runtime);
        // Block cache miss requests in new detlta protocol
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> deltaBlock(runtime);
        TBlockEvents<TEvNodeBroker::TEvSyncNodesRequest> syncBlock(runtime);

        // Register dynamic nodes in NodeBroker
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
            1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
            1, 2, 3, 5, TStatus::OK, NODE2);

        // Send asynchronous node resolve request to DynamicNameserver with no deadline
        AsyncResolveNode(runtime, sender, NODE1);

        // Send asynchronous node resolve request to DynamicNameserver with deadline
        AsyncResolveNode(runtime, sender, NODE2, TDuration::Seconds(1));

        // Wait until cache misses are blocked
        runtime.WaitFor("cache miss", [&]{ return resolveBlock.size() >= 2 || syncBlock.size() >= 1; });

        // Move time to the deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve request is failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE2);

        // Unblock blocked cache misses
        resolveBlock.Unblock();
        deltaBlock.Unblock();
        syncBlock.Unblock();

        // Resolve request with no deadline is OK
        CheckAsyncResolveNode(runtime, NODE1, "1.2.3.4");

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST_FLAG(CacheMissDifferentDeadline, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();

        // Block cache miss requests in old protocol
        TBlockEvents<TEvNodeBroker::TEvResolveNode> resolveBlock(runtime);
        // Block cache miss requests in new detlta protocol
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> deltaBlock(runtime);
        TBlockEvents<TEvNodeBroker::TEvSyncNodesRequest> syncBlock(runtime);

        // Register dynamic nodes in NodeBroker
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
            1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
            1, 2, 3, 5, TStatus::OK, NODE2);

        // Send asynchronous node resolve requests to DynamicNameserver with different deadline
        AsyncResolveNode(runtime, sender, NODE1, TDuration::Seconds(1));
        AsyncResolveNode(runtime, sender, NODE2, TDuration::Seconds(2));

        // Wait until cache misses are blocked
        runtime.WaitFor("cache miss", [&]{ return resolveBlock.size() >= 2 || syncBlock.size() >= 1; });

        // Move time to the first deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve request is failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE1);

        // Move time to the second deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve request is failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE2);

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
    }

    Y_UNIT_TEST_FLAG(CacheMissDifferentDeadlineInverseOrder, EnableNodeBrokerDeltaProtocol) {
        TTestBasicRuntime runtime(1, false);
        SetupWithDeltaProtocol(runtime, EnableNodeBrokerDeltaProtocol);
        TActorId sender = runtime.AllocateEdgeActor();

        // Block cache miss requests in old protocol
        TBlockEvents<TEvNodeBroker::TEvResolveNode> resolveBlock(runtime);
        // Block cache miss requests in new detlta protocol
        TBlockEvents<TEvNodeBroker::TEvUpdateNodes> deltaBlock(runtime);
        TBlockEvents<TEvNodeBroker::TEvSyncNodesRequest> syncBlock(runtime);

        // Register dynamic nodes in NodeBroker
        CheckRegistration(runtime, sender, "host1", 1001, "host1.host1.host1", "1.2.3.4",
            1, 2, 3, 4, TStatus::OK, NODE1);
        CheckRegistration(runtime, sender, "host2", 1001, "host2.host2.host2", "1.2.3.5",
            1, 2, 3, 5, TStatus::OK, NODE2);

        // Send asynchronous node resolve requests to DynamicNameserver with different deadline
        AsyncResolveNode(runtime, sender, NODE1, TDuration::Seconds(2));
        AsyncResolveNode(runtime, sender, NODE2, TDuration::Seconds(1));

        // Wait until cache misses are blocked
        runtime.WaitFor("cache miss", [&]{ return resolveBlock.size() >= 2 || syncBlock.size() >= 1; });

        // Move time to the first deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve request is failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE2);

        // Move time to the second deadline
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Resolve request is failed, because of deadline
        CheckAsyncResolveUnknownNode(runtime, NODE1);

        // No pending cache miss requests are left
        CheckNoPendingCacheMissesLeft(runtime, 0);
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

Y_UNIT_TEST_SUITE(GracefulShutdown) {
    Y_UNIT_TEST(TTxGracefulShutdown) {
        TTestBasicRuntime runtime(8, false);
        Setup(runtime, 4);
        TActorId sender = runtime.AllocateEdgeActor();

        auto epoch = GetEpoch(runtime, sender);

        CheckRegistration(runtime, sender, "host1", 1001, "host1.yandex.net", "1.2.3.4",
                          1, 2, 3, 4, TStatus::OK, NODE1, epoch.GetNextEnd(),
                          false, DOMAIN_NAME, {}, "slot-0");

        CheckGracefulShutdown(runtime, sender, NODE1);    

        CheckRegistration(runtime, sender, "host2", 1001, "host2.yandex.net", "1.2.3.5",
                          1, 2, 3, 5, TStatus::OK, NODE2, epoch.GetNextEnd(),
                          false, DOMAIN_NAME, {}, "slot-0");
    }
}

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TUpdate, out, value) {
    switch (value.Type) {
        case NKikimr::TUpdate::EType::Updated:
            out << value.NodeId;
            return;
        case NKikimr::TUpdate::EType::Removed:
            out << "R(" << value.NodeId << ")";
            return;
        case NKikimr::TUpdate::EType::Expired:
            out << "E(" << value.NodeId << ")";
            return;
        case NKikimr::TUpdate::EType::Unknown:
            out << "Unknown";
            return;
    }
    out << "Unknown";
}
