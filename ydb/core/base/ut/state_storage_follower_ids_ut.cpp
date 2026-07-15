#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/tx.h>

using namespace NActors;
using namespace NKikimr;

namespace {

/**
 * The number of nodes allocated for the test runtime.
 */
const ui32 TEST_NODE_COUNT = 3;

/**
 * The number of followers created for each node.
 */
const ui32 FOLLOWER_COUNT_PER_NODE = 3;

/**
 * The total number of followers to be created for the test tablet.
 */
const ui32 TOTAL_FOLLOWER_COUNT = TEST_NODE_COUNT * FOLLOWER_COUNT_PER_NODE - 1;

/**
 * The domain name used in tests.
 */
const char* TEST_DOMAIN_NAME = "dc-1";

/**
 * Configure the runtime to run Hive for the tests.
 *
 * @param[in] runtime The test runtime to configure
 */
void SetupRuntimeAndHive(TTestBasicRuntime& runtime) {
    // Enable detailed logging for the relevant modules
    runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::STATESTORAGE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, NLog::PRI_DEBUG);

    // Configure all the services, which are needed to run Hive
    {
        TAppPrepare app;

        // Configure domains
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            TEST_DOMAIN_NAME,
            TTestTxConfig::DomainUid,
            TTestTxConfig::SchemeShard,
            50 /* planResolution */,
            TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
            TVector<ui64>{},
            TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
            DefaultPoolKinds(2)
        );

        TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
        ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
        runtime.SetTxAllocatorTabletIds(ids);

        app.AddDomain(domain.Release());
        app.AddHive(MakeDefaultHiveID());

        // Configure channels
        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles();
        channelProfiles->Profiles.emplace_back();
        TChannelProfiles::TProfile& profile = channelProfiles->Profiles.back();

        for (ui32 channelIndex = 0; channelIndex < 3; ++channelIndex) {
            profile.Channels.push_back(
                TChannelProfiles::TProfile::TChannel(
                    TBlobStorageGroupType::ErasureNone,
                    0,
                    NKikimrBlobStorage::TVDiskKind::Default
                )
            );
        }

        app.SetChannels(std::move(channelProfiles));

        app.SetMinRequestSequenceSize(10);
        app.SetRequestSequenceSize(10);
        app.SetHiveStoragePoolFreshPeriod(0);

        app.HiveConfig.SetMaxNodeUsageToKick(0.9);
        app.HiveConfig.SetMinCounterScatterToBalance(0.02);
        app.HiveConfig.SetMinScatterToBalance(0.5);
        app.HiveConfig.SetObjectImbalanceToBalance(0.02);
        app.HiveConfig.SetScaleInWindowSize(1);
        app.HiveConfig.SetScaleOutWindowSize(1);

        // Configure node warden
        THashMap<ui32, TIntrusivePtr<TNodeWardenConfig>> allNodeWardenConfigs;

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig = new TNodeWardenConfig(
                !runtime.IsRealThreads()
                    ? static_cast<IPDiskServiceFactory*>(new TStrandedPDiskServiceFactory(runtime))
                    : static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory())
            );

            auto serviceSet = nodeWardenConfig->BlobStorageConfig.MutableServiceSet();
            serviceSet->AddAvailabilityDomains(0);

            for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
                auto pdisk = serviceSet->AddPDisks();
                pdisk->SetNodeID(runtime.GetNodeId(i));
                pdisk->SetPDiskID(1);
                pdisk->SetPDiskGuid(i + 1);
                pdisk->SetPath(TStringBuilder() << "SectorMap:" << i << ":3200");
            }

            auto vdisk = serviceSet->AddVDisks();

            vdisk->MutableVDiskID()->SetGroupID(0);
            vdisk->MutableVDiskID()->SetGroupGeneration(1);
            vdisk->MutableVDiskID()->SetRing(0);
            vdisk->MutableVDiskID()->SetDomain(0);
            vdisk->MutableVDiskID()->SetVDisk(0);
            vdisk->MutableVDiskLocation()->SetNodeID(runtime.GetNodeId(0));
            vdisk->MutableVDiskLocation()->SetPDiskID(1);
            vdisk->MutableVDiskLocation()->SetPDiskGuid(1);
            vdisk->MutableVDiskLocation()->SetVDiskSlotID(0);

            auto staticGroup = serviceSet->AddGroups();

            staticGroup->SetGroupID(0);
            staticGroup->SetGroupGeneration(1);
            staticGroup->SetErasureSpecies(0);
            staticGroup->AddRings()->AddFailDomains()->AddVDiskLocations()->CopyFrom(vdisk->GetVDiskLocation());

            allNodeWardenConfigs[nodeIndex] = nodeWardenConfig;
        }

        // Configure PDisks
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig = allNodeWardenConfigs[nodeIndex];

            const TString pDiskPath = TStringBuilder() << "SectorMap:" << nodeIndex << ":3200";
            const ui64 pDiskSize = 32ull << 30ull;
            TIntrusivePtr<NPDisk::TSectorMap> sectorMap = new NPDisk::TSectorMap(pDiskSize);

            nodeWardenConfig->SectorMaps[pDiskPath] = sectorMap;

            FormatPDisk(
                pDiskPath,
                pDiskSize,
                4 << 10,
                32u << 20u,
                nodeIndex + 1,
                0x1234567890,
                0x4567890123,
                0x7890123456,
                NPDisk::YdbDefaultPDiskSequence,
                TString(""),
                {
                    .SectorMap = sectorMap,
                    .EnableSmallDiskOptimization = false,
                }
            );
        }

        // Configure local tablets
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            auto localConfig = new TLocalConfig();

            localConfig->TabletClassInfo[TTabletTypes::Dummy].SetupInfo = new TTabletSetupInfo(
                &CreateFlatDummyTablet,
                TMailboxType::Simple,
                0,
                TMailboxType::Simple,
                0
            );

            localConfig->TabletClassInfo[TTabletTypes::Hive].SetupInfo = new TTabletSetupInfo(
                &CreateDefaultHive,
                TMailboxType::Simple,
                0,
                TMailboxType::Simple,
                0
            );

            localConfig->TabletClassInfo[TTabletTypes::Mediator].SetupInfo = new TTabletSetupInfo(
                &CreateTxMediator,
                TMailboxType::Simple,
                0,
                TMailboxType::Simple,
                0
            );

            localConfig->TabletClassInfo[TTabletTypes::ColumnShard].SetupInfo = new TTabletSetupInfo(
                &CreateColumnShard,
                TMailboxType::Simple,
                0,
                TMailboxType::Simple,
                0
            );

            TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
            tenantPoolConfig->AddStaticSlot(TEST_DOMAIN_NAME);

            runtime.AddLocalService(
                MakeTenantPoolRootID(),
                TActorSetupCmd(
                    CreateTenantPool(tenantPoolConfig),
                    TMailboxType::Revolving,
                    0
                ),
                nodeIndex
            );
        }

        // Configure basic services
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            SetupStateStorage(runtime, nodeIndex);
            SetupBSNodeWarden(runtime, nodeIndex, allNodeWardenConfigs[nodeIndex]);
            SetupTabletResolver(runtime, nodeIndex);
            SetupNodeWhiteboard(runtime, nodeIndex);
        }

        runtime.Initialize(app.Unwrap());
    }

    // Enable scheduling
    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        runtime.EnableScheduleForActor(
            runtime.GetLocalServiceId(
                MakeLocalID(runtime.GetNodeId(nodeIndex)),
                nodeIndex
            )
        );

        runtime.EnableScheduleForActor(
            runtime.GetLocalServiceId(
                MakeBlobStorageNodeWardenID(runtime.GetNodeId(nodeIndex)),
                nodeIndex
            )
        );

        runtime.EnableScheduleForActor(
            runtime.GetLocalServiceId(
                MakeTabletResolverID(),
                nodeIndex
            )
        );
    }

    // Wait for everything to start
    if (!runtime.IsRealThreads()) {
        TDispatchOptions options;
        options.FinalEvents.push_back(
            TDispatchOptions::TFinalEventCondition(
                TEvBlobStorage::EvLocalRecoveryDone,
                1
            )
        );

        runtime.DispatchEvents(options);
    }

    CreateTestBootstrapper(
        runtime,
        CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController),
        &CreateFlatBsController
    );

    // Configure the storage pool
    const TActorId senderActorId = runtime.AllocateEdgeActor();

    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

    runtime.Send(
        new IEventHandle(
            GetNameserviceActorId(),
            senderActorId,
            new TEvInterconnect::TEvListNodes
        )
    );

    TAutoPtr<IEventHandle> handleNodesInfo;
    auto nodesInfo = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handleNodesInfo);

    auto bsConfigureRequest = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();

    NKikimrBlobStorage::TDefineBox boxConfig;
    boxConfig.SetBoxId(1);

    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        ui32 nodeId = runtime.GetNodeId(nodeIndex);
        Y_ABORT_UNLESS(nodesInfo->Nodes[nodeIndex].NodeId == nodeId);
        auto& nodeInfo = nodesInfo->Nodes[nodeIndex];

        NKikimrBlobStorage::TDefineHostConfig hostConfig;

        hostConfig.SetHostConfigId(nodeId);
        hostConfig.AddDrive()->SetPath(TStringBuilder() << "SectorMap:" << nodeIndex << ":3200");

        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineHostConfig()->CopyFrom(hostConfig);

        auto host = boxConfig.AddHost();

        host->MutableKey()->SetFqdn(nodeInfo.Host);
        host->MutableKey()->SetIcPort(nodeInfo.Port);
        host->SetHostConfigId(hostConfig.GetHostConfigId());
    }

    bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineBox()->CopyFrom(boxConfig);

    for (ui64 i = 1; i <= 3; ++i) {
        NKikimrBlobStorage::TDefineStoragePool storagePool;

        storagePool.SetBoxId(1);
        storagePool.SetStoragePoolId(i);
        storagePool.SetName("def" + ToString(i));
        storagePool.SetErasureSpecies("none");
        storagePool.SetVDiskKind("Default");
        storagePool.SetKind("DefaultStoragePool");
        storagePool.SetNumGroups(1);
        storagePool.AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);

        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(storagePool);
    }

    runtime.SendToPipe(MakeBSControllerID(), senderActorId, bsConfigureRequest.Release(), 0, pipeConfig);

    TAutoPtr<IEventHandle> handleConfigureResponse;
    auto configureResponse = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handleConfigureResponse);

    if (!configureResponse->Record.GetResponse().GetSuccess()) {
        Cerr << "\n\n configResponse is #" << configureResponse->Record.DebugString() << "\n\n";
    }

    UNIT_ASSERT(configureResponse->Record.GetResponse().GetSuccess());
}

/**
 * Send the TEvCreateTablet message and process the results.
 *
 * @param[in] runtime The test runtime
 * @param[in] hiveTabletId The ID for the Hive tablet
 * @param[in] testerTabletId The ID for the tester tablet
 * @param[in] ev The TEvCreateTablet event to send
 * @param[in] nodeIndex The node on which to create the tablet
 *
 * @return The ID of the created tablet
 */
ui64 SendCreateTabletMessage(
    TTestActorRuntime &runtime,
    ui64 hiveTabletId,
    ui64 testerTabletId,
    THolder<TEvHive::TEvCreateTablet> ev,
    ui32 nodeIndex
) {
    const TActorId senderActorId = runtime.AllocateEdgeActor(nodeIndex);
    runtime.SendToPipe(hiveTabletId, senderActorId, ev.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);

    UNIT_ASSERT(createTabletReply != nullptr);

    UNIT_ASSERT_EQUAL_C(
        createTabletReply->Record.GetStatus(),
        NKikimrProto::OK,
        (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK
    );

    UNIT_ASSERT_EQUAL_C(
        createTabletReply->Record.GetOwner(),
        testerTabletId,
        createTabletReply->Record.GetOwner() << " != " << testerTabletId
    );

    ui64 tabletId = createTabletReply->Record.GetTabletID();
    for (;;) {
        auto tabletCreationResult = runtime.GrabEdgeEventRethrow<TEvHive::TEvTabletCreationResult>(handle);
        UNIT_ASSERT(tabletCreationResult != nullptr);

        if (tabletId == tabletCreationResult->Record.GetTabletID()) {
            UNIT_ASSERT_EQUAL_C(
                tabletCreationResult->Record.GetStatus(),
                NKikimrProto::OK,
                (ui32)tabletCreationResult->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK
            );

            break;
        }
    }

    return tabletId;
}

/**
 * Create the TChannelBind object for the given storage pool.
 *
 * @param[in] storagePool The name of the storage pool
 *
 * @return The corresponding TChannelBind object
 */
TChannelBind CreateChannelBind(const TString& storagePool) {
    TChannelBind bind;
    bind.SetStoragePoolName(storagePool);
    return bind;
}

/**
 * Start a new tablet with a follower on each test node.
 *
 * @param[in] runtime The test runtime
 * @param[out] tabletId The ID of the created tablet
 * @param[out] leaderNodeIndex The index of the node where the leader is running
 */
void StartTabletWithFollowers(
    TTestBasicRuntime& runtime,
    ui64& tabletId,
    ui32& leaderNodeIndex
) {
    const ui64 hiveTabletId = MakeDefaultHiveID();
    const ui64 testerTabletId = MakeTabletID(false, 1);

    CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTabletId, TTabletTypes::Hive), &CreateDefaultHive);

    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
        runtime.DispatchEvents(options);
    }

    // Create a tablet with a follower on each node
    THolder<TEvHive::TEvCreateTablet> ev(
        new TEvHive::TEvCreateTablet(
            testerTabletId,
            100500,
            TTabletTypes::Dummy,
            {
                CreateChannelBind("def1"),
                CreateChannelBind("def2"),
                CreateChannelBind("def3"),
            }
        )
    );

    auto followerGroup = ev->Record.AddFollowerGroups();
    followerGroup->SetFollowerCount(TOTAL_FOLLOWER_COUNT);
    followerGroup->SetAllowLeaderPromotion(true);

    tabletId = SendCreateTabletMessage(runtime, hiveTabletId, testerTabletId, std::move(ev), 0);

    // Build a translation from the node ID to node index
    std::unordered_map<ui32, ui32> nodeIdToNodeIndex;

    for (ui32 nodeIndex = 0; nodeIndex < TEST_NODE_COUNT; ++nodeIndex) {
        nodeIdToNodeIndex[runtime.GetNodeId(nodeIndex)] = nodeIndex;
    }

    // Wait for the leader and all followers to start
    const TActorId senderActorId = runtime.AllocateEdgeActor();

    for (;;) {
        runtime.Send(
            new IEventHandle(
                MakeStateStorageProxyID(),
                senderActorId,
                new TEvStateStorage::TEvLookup(tabletId, 123456789)
            )
        );

        auto tabletInfo = runtime.GrabEdgeEventRethrow<TEvStateStorage::TEvInfo>(senderActorId)->Release();

        UNIT_ASSERT(tabletInfo != nullptr);
        UNIT_ASSERT_EQUAL(tabletInfo->TabletID, tabletId);

        if (tabletInfo->Followers.size() == TOTAL_FOLLOWER_COUNT) {
            leaderNodeIndex = nodeIdToNodeIndex[tabletInfo->CurrentLeaderTablet.NodeId()];
            break;
        }
    }
}

/**
 * Retrieve the information about the given tablet from the State Storage.
 *
 * @param[in] runtime The test runtime
 * @param[in] tabletId The ID of the tablet for which to retrieve the data
 * @param[in] excludedNodeIndex If specified, no followers should run on this node
 *
 * @return The data for this tablet from the State Storage
 */
TAutoPtr<TEvStateStorage::TEvInfo> LookupTabletInfo(
    TTestBasicRuntime& runtime,
    ui64 tabletId,
    const TMaybe<ui32>& excludedNodeIndex = TMaybe<ui32>()
) {
    const TActorId senderActorId = runtime.AllocateEdgeActor();

    runtime.Send(
        new IEventHandle(
            MakeStateStorageProxyID(),
            senderActorId,
            new TEvStateStorage::TEvLookup(tabletId, 123456789)
        )
    );

    auto tabletInfo = runtime.GrabEdgeEventRethrow<TEvStateStorage::TEvInfo>(senderActorId)->Release();
    UNIT_ASSERT(tabletInfo != nullptr);

    Cerr << "---- TEST ---- Received TEvInfo\n" << tabletInfo->ToString() << "\n";

    // Make sure the leader and all followers are spread across all nodes
    UNIT_ASSERT_EQUAL(tabletInfo->TabletID, tabletId);
    UNIT_ASSERT_EQUAL(tabletInfo->Followers.size(), TOTAL_FOLLOWER_COUNT);

    std::unordered_set<ui32> nodeIds = {tabletInfo->CurrentLeader.NodeId()};
    std::unordered_set<ui32> expectedNodeIds;

    for (size_t i = 0; i < TEST_NODE_COUNT; ++i) {
        if (excludedNodeIndex != i) {
            expectedNodeIds.insert(runtime.GetNodeId(i));
        }
    }

    for (const auto& followerInfo : tabletInfo->Followers) {
        nodeIds.insert(followerInfo.Follower.NodeId());
    }

    UNIT_ASSERT_EQUAL_C(nodeIds, expectedNodeIds, "All node IDs must be unique");

    return tabletInfo;
}

/**
 * Send a ping to the given tablet leader/follower by sending
 * the TEvRemoteHttpInfo message through the tablet pipe.
 *
 * @param[in] runtime The test runtime
 * @param[in] tabletId The ID of the tablet for which to retrieve the data
 * @param[in] followerId The ID of the follower to which to connect (0 == leader)
 * @param[in] expectConnectError If true, expect the connect request to fail
 * @param[in] impliedFollowerId If true, only AllowFollower flag is set, not FollowerId
 * @param[in] senderNodeId If impliedFollowerId is set, specifies the node ID for sending requests
 *
 * @return The ID of the actor, which responded with the TEvRemoteHttpInfoRes response
 *         (will be set to an empty ID if there is a connection error)
 */
TActorId SendPingThroughTabletPipe(
    TTestBasicRuntime& runtime,
    ui64 tabletId,
    ui32 followerId,
    bool expectConnectError = false,
    bool impliedFollowerId = false,
    ui32 senderNodeId = 0
) {
    // NOTE: If an explicit follower ID is requested, always connect to node 0,
    //       because it should not matter. For the implicit leader/follower selection
    //       connect to the same node as the follower ID because the Tablet Resolver
    //       always prefers local tablets.
    ui32 senderNodeIndex = (impliedFollowerId) ? TEST_NODE_COUNT : 0;

    if (impliedFollowerId) {
        // Translate the node ID to the corresponding node index
        for (ui32 i = 0; i < TEST_NODE_COUNT; ++i) {
            if (runtime.GetNodeId(i) == senderNodeId) {
                senderNodeIndex = i;
                break;
            }
        }

        UNIT_ASSERT_C(
            senderNodeIndex < TEST_NODE_COUNT,
            "Invalid node ID for sending tablet pipe requests: " << senderNodeId
        );
    }

    const TActorId senderActorId = runtime.AllocateEdgeActor(senderNodeIndex);

    // NOTE: Sending requests with an explicit follower ID to connect
    //       to a specific leader/follower may fail because the given follower ID
    //       either does not exist or may not be known yet. To avoid infinite loops,
    //       the pipe must be configured with a very small number of retries
    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = {.RetryLimitCount = 2};

    if (!impliedFollowerId) {
        pipeConfig.FollowerId = followerId;
    } else {
        pipeConfig.AllowFollower = (followerId != 0);
    }

    // NOTE: This test creates tablets of the type TTabletTypes::Dummy,
    //       which is mapped to TDummyFlatTablet. And this tablet does not respond
    //       to anything other than the most basic standard messages.
    //       The code below sends NMon::TEvRemoteHttpInfo and gets back
    //       NMon::TEvRemoteHttpInfoRes. There is no specific meaning
    //       in those message here - the point is to get back the actor ID
    //       of the tablet (which responded) to make sure the message was routed
    //       to the correct leader/follower.
    runtime.SendToPipe(
        tabletId,
        senderActorId,
        new NMon::TEvRemoteHttpInfo(),
        senderNodeIndex,
        pipeConfig
    );

    // When the client is unable to connect,
    // it sends back the TEvClientConnected message with the status set to ERROR
    if (expectConnectError) {
        auto response = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(senderActorId);
        UNIT_ASSERT(response != nullptr);

        Cerr << "---- TEST ---- Received TEvClientConnected " << response->ToString() << "\n";
        UNIT_ASSERT_EQUAL(response->Get()->Status, NKikimrProto::ERROR);

        return TActorId();
    }

    auto response = runtime.GrabEdgeEventRethrow<NMon::TEvRemoteHttpInfoRes>(senderActorId);
    UNIT_ASSERT(response != nullptr);

    Cerr << "---- TEST ---- Received TEvRemoteHttpInfoRes from " << response->Sender << "\n";
    return response->Sender;
}

/**
 * Kill the given node and wait for the leader/follower to be restarted or promoted.
 *
 * @param[in] runtime The test runtime
 * @param[in] nodeIndex The index of the node to kill
 * @param[in] expectLeaderPromotion If true, a leader promotion is expected,
 *                                  otherwise a follower restart is expected
 */
void KillNodeAndWaitForRestart(
    TTestBasicRuntime& runtime,
    ui32 nodeIndex,
    bool expectLeaderPromotion
) {
    const auto nodeId = runtime.GetNodeId(nodeIndex);

    Cerr << "---- TEST ---- Killing node " << nodeIndex
        << ", node ID " << nodeId
        << "\n";

    runtime.Send(
        new IEventHandle(
            MakeLocalID(nodeId),
            TActorId(),
            new TEvents::TEvPoisonPill()
        ),
        nodeIndex
    );

    // Wait for the leader to promoted or the follower to be restarted
    //
    // NOTE: When handling the follower promotion scenario,
    //       the EvTabletStatus message will be sent for the following events
    //
    //         * Stopping one of the followers (to be promoted to the leader)
    //         * Starting the new leader (promoted from one of the follower)
    //         * Starting a new follower (as a replacement for the promoted follower)
    //
    // NOTE: When handling the follower restart scenario,
    //       the EvTabletStatus message will be sent for the following events
    //
    //         * Starting a new follower (as a replacement for the killed follower)
    TDispatchOptions options;

    options.FinalEvents.emplace_back(
        TEvLocal::EvTabletStatus,
        (expectLeaderPromotion)
            ? 3 + FOLLOWER_COUNT_PER_NODE - 1
            : FOLLOWER_COUNT_PER_NODE
    );

    runtime.DispatchEvents(options);
}

} // namespace <anonymous>

namespace NKikimr {

/**
 * Unit tests, which verify the parts of the State Storage,
 * which are related to collecting follower IDs.
 */
Y_UNIT_TEST_SUITE(TStateStorageFollowerIDsTest) {
    /**
     * Verify that follower IDs are reported correctly by the State Storage Proxy.
     * in the case when all nodes are correctly reporting follower IDs.
     */
    Y_UNIT_TEST(ProxyCorrectFollowerIds) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Get the information for this tablet from State Storage
        // and make sure all follower IDs are populated and correct
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        std::unordered_set<ui32> followerIds;
        std::unordered_set<ui32> expectedFollowerIds;

        for (size_t i = 0; i < tabletInfo->Followers.size(); ++i) {
            expectedFollowerIds.insert(i + 1);
            const auto& followerInfo = tabletInfo->Followers[i];

            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerIds.insert(followerInfo.FollowerId.Get());
        }

        UNIT_ASSERT_EQUAL_C(followerIds, expectedFollowerIds, "All follower IDs must be unique");
    }

    /**
     * Verify that follower IDs are resolved correctly by the Tablet Resolver
     * (when the table pipe client is used to send messages to the tablet)
     * in the case when all nodes are correctly reporting follower IDs.
     */
    Y_UNIT_TEST(ResolverCorrectFollowerIds) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Figure out the translation from FollowerId to the tablet actor ID
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        std::unordered_map<ui32, TActorId> followerActors = {
            {0, tabletInfo->CurrentLeaderTablet}, // The leader is not in the list of followers!
        };

        for (const auto& followerInfo : tabletInfo->Followers) {
            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerActors[followerInfo.FollowerId.Get()] = followerInfo.FollowerTablet;
        }

        UNIT_ASSERT_EQUAL(followerActors.size(), TOTAL_FOLLOWER_COUNT + 1);

        // Send a request to ping the tablet to each follower (and the leader)
        // to verify that the Tablet Resolver correctly resolves follower IDs
        for (ui32 followerId = 0; followerId <= TOTAL_FOLLOWER_COUNT; ++followerId) {
            Cerr << "---- TEST ---- Resolving followerId " << followerId << "\n";

            auto tabletActorId = SendPingThroughTabletPipe(runtime, tabletId, followerId);

            UNIT_ASSERT_EQUAL_C(
                tabletActorId,
                followerActors[followerId],
                "Selected actor " << tabletActorId << ", expected " << followerActors[followerId]
            );
        }
    }

    /**
     * Verify that the Tablet Resolver correctly resolves the leader implicitly
     * when the client does not specify an explicit follower ID == 0
     * (when the table pipe client is used to send messages to the tablet)
     * in the case when all nodes are correctly reporting follower IDs.
     */
    Y_UNIT_TEST(ResolverImplicitLeader) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Figure out the translation from FollowerId to the tablet actor ID
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        // Send a request to ping the leader implicitly (without the follower ID)
        Cerr << "---- TEST ---- Resolving the leader implicitly\n";

        auto tabletActorId = SendPingThroughTabletPipe(
            runtime,
            tabletId,
            0 /* followerId */,
            false /* expectConnectError */,
            true /* impliedFollowerId */,
            runtime.GetNodeId(leaderNodeIndex)
        );

        UNIT_ASSERT_EQUAL_C(
            tabletActorId,
            tabletInfo->CurrentLeaderTablet,
            "Selected actor " << tabletActorId << ", expected " << tabletInfo->CurrentLeaderTablet
        );
    }

    /**
     * Verify that the Tablet Resolver correctly resolves the followers implicitly
     * when the client does not specify an explicit follower ID != 0
     * (when the table pipe client is used to send messages to the tablet)
     * in the case when all nodes are correctly reporting follower IDs.
     */
    Y_UNIT_TEST(ResolverImplicitFollowers) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Figure out the translation from FollowerId to the tablet actor ID
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        std::unordered_set<TActorId> followerTabletIds = {
            tabletInfo->CurrentLeaderTablet, // The leader is not in the list of followers!
        };

        std::unordered_map<ui32, TActorId> followerActors = {
            {0, tabletInfo->CurrentLeaderTablet}, // The leader is not in the list of followers!
        };

        std::unordered_map<TActorId, ui32> followerTabletIdToFollowerId = {
            {tabletInfo->CurrentLeaderTablet, 0}, // The leader is not in the list of followers!
        };

        for (const auto& followerInfo : tabletInfo->Followers) {
            UNIT_ASSERT(followerInfo.FollowerId.Defined());

            followerTabletIds.insert(followerInfo.FollowerTablet);
            followerActors[followerInfo.FollowerId.Get()] = followerInfo.FollowerTablet;
            followerTabletIdToFollowerId[followerInfo.FollowerTablet] = followerInfo.FollowerId.Get();
        }

        UNIT_ASSERT_EQUAL(followerTabletIds.size(), TOTAL_FOLLOWER_COUNT + 1);
        UNIT_ASSERT_EQUAL(followerActors.size(), TOTAL_FOLLOWER_COUNT + 1);
        UNIT_ASSERT_EQUAL(followerTabletIdToFollowerId.size(), TOTAL_FOLLOWER_COUNT + 1);

        // Send a request to ping the tablet to any follower/leader (implicitly),
        // but since the follower selection is random, repeat this many times
        // and expect all followers to be chosen eventually
        std::unordered_set<TActorId> chosenFollowerTabletIds;

        for (ui32 i = 0; i < 100; ++i) {
            for (ui32 followerId = 0; followerId <= TOTAL_FOLLOWER_COUNT; ++followerId) {
                Cerr << "---- TEST ---- Resolving followerId " << followerId
                    << ", iteration " << i
                    << "\n";

                auto tabletActorId = SendPingThroughTabletPipe(
                    runtime,
                    tabletId,
                    followerId,
                    false /* expectConnectError */,
                    true /* impliedFollowerId */,
                    followerActors[followerId].NodeId()
                );

                Cerr << "---- TEST ---- Resolved followerId " << followerId
                    << ", tabletId " << tabletActorId
                    << ", followerId " << followerTabletIdToFollowerId[tabletActorId]
                    << "\n";

                chosenFollowerTabletIds.insert(tabletActorId);
            }
        }

        UNIT_ASSERT_EQUAL_C(
            followerTabletIds,
            chosenFollowerTabletIds,
            "Not all followers were chosen implicitly"
        );
    }

    /**
     * Verify that follower IDs are reported correctly by the State Storage Proxy
     * in the case when some nodes are not reporting follower IDs
     * when registering followers with the State Storage
     * (see the NKikimrStateStorage::TEvRegisterFollower message).
     */
    Y_UNIT_TEST(ProxyNoFollowerIdInFollowerRegistration) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        // Drop the FollowerId field from the TEvRegisterFollower messages
        // for one of the followers
        TActorId patchedFollowerActorId;

        auto observerHolder = runtime.AddObserver<TEvStateStorage::TEvReplicaRegFollower>(
            [&patchedFollowerActorId](TEvStateStorage::TEvReplicaRegFollower::TPtr& ev) {
                const auto msg = ev->Get();

                UNIT_ASSERT(msg->Record.HasFollowerId());

                if (msg->Record.GetFollowerId() == 1) {
                    Cerr << "---- TEST ---- Received TEvRegisterFollower\n"
                        << msg->Record.DebugString()
                        << "\n";

                    msg->Record.ClearFollowerId();
                    patchedFollowerActorId = ActorIdFromProto(msg->Record.GetFollower());

                    Cerr << "---- TEST ---- Patched TEvRegisterFollower\n"
                        << msg->Record.DebugString()
                        << "\n";
                }
            }
        );

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Get the information for this tablet from State Storage
        // and make sure all follower IDs are populated and correct
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        std::unordered_set<ui32> followerIds;
        std::unordered_set<ui32> expectedFollowerIds;

        for (size_t i = 0; i < tabletInfo->Followers.size(); ++i) {
            expectedFollowerIds.insert(i + 1);
            const auto& followerInfo = tabletInfo->Followers[i];

            if (patchedFollowerActorId == followerInfo.Follower) {
                UNIT_ASSERT(!followerInfo.FollowerId.Defined());
                continue;
            }

            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerIds.insert(followerInfo.FollowerId.Get());
        }

        expectedFollowerIds.erase(1); // This followerId patched by the observer
        UNIT_ASSERT_EQUAL_C(followerIds, expectedFollowerIds, "All follower IDs must be unique");
    }

    /**
     * Verify that follower IDs are resolved correctly by the Tablet Resolver
     * (when the table pipe client is used to send messages to the tablet)
     * in the case when some nodes are not reporting follower IDs
     * when registering followers with the State Storage
     * (see the NKikimrStateStorage::TEvRegisterFollower message).
     */
    Y_UNIT_TEST(ResolverNoFollowerIdInFollowerRegistration) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        // Drop the FollowerId field from the TEvRegisterFollower messages
        // for one of the followers
        TActorId patchedFollowerActorId;

        auto observerHolder = runtime.AddObserver<TEvStateStorage::TEvReplicaRegFollower>(
            [&patchedFollowerActorId](TEvStateStorage::TEvReplicaRegFollower::TPtr& ev) {
                const auto msg = ev->Get();

                UNIT_ASSERT(msg->Record.HasFollowerId());

                if (msg->Record.GetFollowerId() == 1) {
                    Cerr << "---- TEST ---- Received TEvRegisterFollower\n"
                        << msg->Record.DebugString()
                        << "\n";

                    msg->Record.ClearFollowerId();
                    patchedFollowerActorId = ActorIdFromProto(msg->Record.GetFollower());

                    Cerr << "---- TEST ---- Patched TEvRegisterFollower\n"
                        << msg->Record.DebugString()
                        << "\n";
                }
            }
        );

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Figure out the translation from FollowerId to the tablet actor ID
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        std::unordered_map<ui32, TActorId> followerActors = {
            {0, tabletInfo->CurrentLeaderTablet}, // The leader is not in the list of followers!
        };

        for (const auto& followerInfo : tabletInfo->Followers) {
            if (patchedFollowerActorId == followerInfo.Follower) {
                UNIT_ASSERT(!followerInfo.FollowerId.Defined());
                continue;
            }

            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerActors[followerInfo.FollowerId.Get()] = followerInfo.FollowerTablet;
        }

        UNIT_ASSERT_EQUAL(followerActors.size(), TOTAL_FOLLOWER_COUNT);

        // Send a request to ping the tablet to each follower (and the leader)
        // to verify that the Tablet Resolver correctly resolves follower IDs
        for (ui32 followerId = 0; followerId <= TOTAL_FOLLOWER_COUNT; ++followerId) {
            Cerr << "---- TEST ---- Resolving followerId " << followerId << "\n";

            auto followerIdIt = followerActors.find(followerId);
            const bool expectConnectError = followerIdIt == followerActors.end();
            const TActorId expectedActorId = (!expectConnectError) ? followerIdIt->second : TActorId();

            auto tabletActorId = SendPingThroughTabletPipe(
                runtime,
                tabletId,
                followerId,
                expectConnectError
            );

            UNIT_ASSERT_EQUAL_C(
                tabletActorId,
                expectedActorId,
                "Selected actor " << tabletActorId << ", expected " << expectedActorId
            );
        }
    }

    /**
     * Verify that follower IDs are reported correctly by the State Storage Proxy
     * in the case when some nodes are not reporting follower IDs
     * when exchanging information between the State Storage nodes
     * (see the NKikimrStateStorage::TEvInfo message).
     */
    Y_UNIT_TEST(ProxyNoFollowerIdsInStateStorageExchange) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        // Drop the FollowerId field from the TEvInfo messages
        auto observerHolder = runtime.AddObserver<TEvStateStorage::TEvReplicaInfo>(
            [](TEvStateStorage::TEvReplicaInfo::TPtr& ev) {
                const auto msg = ev->Get();

                if (msg->Record.FollowerSize() != 0) {
                    UNIT_ASSERT(msg->Record.FollowerInfoSize() == msg->Record.FollowerSize());

                    Cerr << "---- TEST ---- Received TEvInfo\n"
                        << msg->Record.DebugString()
                        << "\n";

                    msg->Record.ClearFollowerInfo();

                    Cerr << "---- TEST ---- Patched TEvInfo\n"
                        << msg->Record.DebugString()
                        << "\n";
                }
            }
        );

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Get the information for this tablet from State Storage
        // and make sure all follower IDs are not populated
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        for (const auto& followerInfo : tabletInfo->Followers) {
            UNIT_ASSERT(!followerInfo.FollowerId.Defined());
        }
    }

    /**
     * Verify that follower IDs are resolved correctly by the Tablet Resolver
     * (when the table pipe client is used to send messages to the tablet)
     * in the case when some nodes are not reporting follower IDs
     * when exchanging information between the State Storage nodes
     * (see the NKikimrStateStorage::TEvInfo message).
     */
    Y_UNIT_TEST(ResolverNoFollowerIdsInStateStorageExchange) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        // Drop the FollowerId field from the TEvInfo messages
        auto observerHolder = runtime.AddObserver<TEvStateStorage::TEvReplicaInfo>(
            [](TEvStateStorage::TEvReplicaInfo::TPtr& ev) {
                const auto msg = ev->Get();

                if (msg->Record.FollowerSize() != 0) {
                    UNIT_ASSERT(msg->Record.FollowerInfoSize() == msg->Record.FollowerSize());

                    Cerr << "---- TEST ---- Received TEvInfo\n"
                        << msg->Record.DebugString()
                        << "\n";

                    msg->Record.ClearFollowerInfo();

                    Cerr << "---- TEST ---- Patched TEvInfo\n"
                        << msg->Record.DebugString()
                        << "\n";
                }
            }
        );

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Send a request to ping the tablet to each follower (and the leader)
        // to verify that the Tablet Resolver correctly resolves follower IDs
        auto tabletInfo = LookupTabletInfo(runtime, tabletId);

        for (ui32 followerId = 0; followerId <= TOTAL_FOLLOWER_COUNT; ++followerId) {
            Cerr << "---- TEST ---- Resolving followerId " << followerId << "\n";

            const bool expectConnectError = (followerId != 0);
            const TActorId expectedActorId = (!expectConnectError) ? tabletInfo->CurrentLeaderTablet : TActorId();

            auto tabletActorId = SendPingThroughTabletPipe(
                runtime,
                tabletId,
                followerId,
                expectConnectError
            );

            UNIT_ASSERT_EQUAL_C(
                tabletActorId,
                expectedActorId,
                "Selected actor " << tabletActorId << ", expected " << expectedActorId
            );
        }
    }

    /**
     * Verify that follower IDs are reported correctly by the State Storage Proxy.
     * after the leader is killed and one of the followers is promoted to the leader.
     */
    Y_UNIT_TEST(ProxyFollowerPromotion) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Force one of the followers to be promoted
        KillNodeAndWaitForRestart(
            runtime,
            leaderNodeIndex,
            true /* expectLeaderPromotion */
        );

        // Get the information for this tablet from State Storage
        // and make sure all follower IDs are populated and correct
        //
        // NOTE: The node on which the leader used to be running should be excluded
        //       from the list of allowed nodes for the tablet to run on, because
        //       this node is shut down and Hive will restart one of the followers
        //       (the one that was promoted to the leader) on some other node
        auto tabletInfo = LookupTabletInfo(
            runtime,
            tabletId,
            leaderNodeIndex /* excludedNodeIndex */
        );

        std::unordered_set<ui32> followerIds;
        std::unordered_set<ui32> expectedFollowerIds;

        for (size_t i = 0; i < tabletInfo->Followers.size(); ++i) {
            expectedFollowerIds.insert(i + 1);
            const auto& followerInfo = tabletInfo->Followers[i];

            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerIds.insert(followerInfo.FollowerId.Get());
        }

        UNIT_ASSERT_EQUAL_C(followerIds, expectedFollowerIds, "All follower IDs must be unique");
    }

    /**
     * Verify that follower IDs are resolved correctly by the Tablet Resolver
     * (when the table pipe client is used to send messages to the tablet)
     * after the leader is killed and one of the followers is promoted to the leader.
     */
    Y_UNIT_TEST(ResolverFollowerPromotion) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Force one of the followers to be promoted
        KillNodeAndWaitForRestart(
            runtime,
            leaderNodeIndex,
            true /* expectLeaderPromotion */
        );

        // Figure out the translation from FollowerId to the tablet actor ID
        //
        // NOTE: The node on which the leader used to be running should be excluded
        //       from the list of allowed nodes for the tablet to run on, because
        //       this node is shut down and Hive will restart one of the followers
        //       (the one that was promoted to the leader) on some other node
        auto tabletInfo = LookupTabletInfo(
            runtime,
            tabletId,
            leaderNodeIndex /* excludedNodeIndex */
        );

        std::unordered_map<ui32, TActorId> followerActors = {
            {0, tabletInfo->CurrentLeaderTablet}, // The leader is not in the list of followers!
        };

        for (const auto& followerInfo : tabletInfo->Followers) {
            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerActors[followerInfo.FollowerId.Get()] = followerInfo.FollowerTablet;
        }

        UNIT_ASSERT_EQUAL(followerActors.size(), TOTAL_FOLLOWER_COUNT + 1);

        // Send a request to ping the tablet to each follower (and the leader)
        // to verify that the Tablet Resolver correctly resolves follower IDs
        for (ui32 followerId = 0; followerId <= TOTAL_FOLLOWER_COUNT; ++followerId) {
            Cerr << "---- TEST ---- Resolving followerId " << followerId << "\n";

            auto tabletActorId = SendPingThroughTabletPipe(runtime, tabletId, followerId);

            UNIT_ASSERT_EQUAL_C(
                tabletActorId,
                followerActors[followerId],
                "Selected actor " << tabletActorId << ", expected " << followerActors[followerId]
            );
        }
    }

    /**
     * Verify that follower IDs are reported correctly by the State Storage Proxy.
     * after one of the follower is killed and restarted on some other node.
     */
    Y_UNIT_TEST(ProxyFollowerRestart) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Kill one of the followers (on some other node from the leader)
        const ui32 followerNodeIndex = (leaderNodeIndex == 0) ? 1 : 0;

        KillNodeAndWaitForRestart(
            runtime,
            followerNodeIndex,
            false /* expectLeaderPromotion */
        );

        // Get the information for this tablet from State Storage
        // and make sure all follower IDs are populated and correct
        //
        // NOTE: The node on which the follower used to be running should be excluded
        //       from the list of allowed nodes for the tablet to run on, because
        //       this node is shut down and Hive will restart this follower on some other node
        auto tabletInfo = LookupTabletInfo(
            runtime,
            tabletId,
            followerNodeIndex /* excludedNodeIndex */
        );

        std::unordered_set<ui32> followerIds;
        std::unordered_set<ui32> expectedFollowerIds;

        for (size_t i = 0; i < tabletInfo->Followers.size(); ++i) {
            expectedFollowerIds.insert(i + 1);
            const auto& followerInfo = tabletInfo->Followers[i];

            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerIds.insert(followerInfo.FollowerId.Get());
        }

        UNIT_ASSERT_EQUAL_C(followerIds, expectedFollowerIds, "All follower IDs must be unique");
    }

    /**
     * Verify that follower IDs are resolved correctly by the Tablet Resolver
     * (when the table pipe client is used to send messages to the tablet)
     * after one of the follower is killed and restarted on some other node.
     */
    Y_UNIT_TEST(ResolverFollowerRestart) {
        TTestBasicRuntime runtime(TEST_NODE_COUNT, false /* useRealThreads */);
        SetupRuntimeAndHive(runtime);

        ui64 tabletId;
        ui32 leaderNodeIndex;
        StartTabletWithFollowers(runtime, tabletId, leaderNodeIndex);

        // Kill one of the followers (on some other node from the leader)
        const ui32 followerNodeIndex = (leaderNodeIndex == 0) ? 1 : 0;

        KillNodeAndWaitForRestart(
            runtime,
            followerNodeIndex,
            false /* expectLeaderPromotion */
        );

        // Get the information for this tablet from State Storage
        // and make sure all follower IDs are populated and correct
        //
        // NOTE: The node on which the follower used to be running should be excluded
        //       from the list of allowed nodes for the tablet to run on, because
        //       this node is shut down and Hive will restart this follower on some other node
        auto tabletInfo = LookupTabletInfo(
            runtime,
            tabletId,
            followerNodeIndex /* excludedNodeIndex */
        );

        std::unordered_map<ui32, TActorId> followerActors = {
            {0, tabletInfo->CurrentLeaderTablet}, // The leader is not in the list of followers!
        };

        for (const auto& followerInfo : tabletInfo->Followers) {
            UNIT_ASSERT(followerInfo.FollowerId.Defined());
            followerActors[followerInfo.FollowerId.Get()] = followerInfo.FollowerTablet;
        }

        UNIT_ASSERT_EQUAL(followerActors.size(), TOTAL_FOLLOWER_COUNT + 1);

        // Send a request to ping the tablet to each follower (and the leader)
        // to verify that the Tablet Resolver correctly resolves follower IDs
        for (ui32 followerId = 0; followerId <= TOTAL_FOLLOWER_COUNT; ++followerId) {
            Cerr << "---- TEST ---- Resolving followerId " << followerId << "\n";

            auto tabletActorId = SendPingThroughTabletPipe(runtime, tabletId, followerId);

            UNIT_ASSERT_EQUAL_C(
                tabletActorId,
                followerActors[followerId],
                "Selected actor " << tabletActorId << ", expected " << followerActors[followerId]
            );
        }
    }
}

}
