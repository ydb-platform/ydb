#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/core/util/random.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/nodewarden/distconf_quorum.h>

namespace NKikimr {
namespace NBlobStorageNodeWardenTest{

namespace {

    void AddStorageConfigNodes(NKikimrBlobStorage::TStorageConfig& config, ui32 nodeCount) {
        for (ui32 nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(nodeId);
            node->SetHost("node-" + std::to_string(nodeId));
            node->SetPort(19000 + nodeId);
            node->MutableLocation()->SetDataCenter("dc-" + std::to_string(nodeId));
        }
    }

    void AddMultiVDiskGroup(NKikimrBlobStorage::TStorageConfig& config,
                            const std::vector<ui32>& nodeIds,
                            i32 erasureSpecies) {
        auto *serviceSet = config.MutableBlobStorageConfig()->MutableServiceSet();
        auto *group = serviceSet->AddGroups();
        group->SetGroupID(0);
        group->SetGroupGeneration(1);
        group->SetErasureSpecies(erasureSpecies);
        auto *ring = group->AddRings();

        for (ui32 domainIdx = 0; domainIdx < nodeIds.size(); ++domainIdx) {
            const ui32 nodeId = nodeIds[domainIdx];
            const ui32 pdiskId = 1;
            const ui64 pdiskGuid = nodeId * 1000 + pdiskId;

            auto *pdisk = serviceSet->AddPDisks();
            pdisk->SetNodeID(nodeId);
            pdisk->SetPDiskID(pdiskId);
            pdisk->SetPath("/dev/disk" + std::to_string(nodeId) + "_" + std::to_string(pdiskId));
            pdisk->SetPDiskGuid(pdiskGuid);
            pdisk->SetPDiskCategory(0);

            auto *vdisk = serviceSet->AddVDisks();
            auto *vdiskId = vdisk->MutableVDiskID();
            vdiskId->SetGroupID(0);
            vdiskId->SetGroupGeneration(1);
            vdiskId->SetRing(0);
            vdiskId->SetDomain(domainIdx);
            vdiskId->SetVDisk(0);
            auto *vdiskLocation = vdisk->MutableVDiskLocation();
            vdiskLocation->SetNodeID(nodeId);
            vdiskLocation->SetPDiskID(pdiskId);
            vdiskLocation->SetVDiskSlotID(0);
            vdiskLocation->SetPDiskGuid(pdiskGuid);

            auto *failDomain = ring->AddFailDomains();
            auto *groupLocation = failDomain->AddVDiskLocations();
            groupLocation->SetNodeID(nodeId);
            groupLocation->SetPDiskID(pdiskId);
            groupLocation->SetVDiskSlotID(0);
            groupLocation->SetPDiskGuid(pdiskGuid);
        }
    }

    TIntrusivePtr<TNodeWardenConfig> MakeNodeWardenConfig(const NKikimrBlobStorage::TStorageConfig& storageConfig) {
        auto config = MakeIntrusive<TNodeWardenConfig>(TIntrusivePtr<IPDiskServiceFactory>());
        config->NameserviceConfig = std::make_unique<NKikimrConfig::TStaticNameserviceConfig>();
        for (const auto& node : storageConfig.GetAllNodes()) {
            auto *nsNode = config->NameserviceConfig->AddNode();
            nsNode->SetNodeId(node.GetNodeId());
            nsNode->SetHost(node.GetHost());
            nsNode->SetInterconnectHost(node.GetHost());
            nsNode->SetPort(node.GetPort());
            nsNode->MutableLocation()->CopyFrom(node.GetLocation());
        }
        return config;
    }

    class TIgnoreActor : public TActor<TIgnoreActor> {
    public:
        TIgnoreActor()
            : TActor(&TThis::StateFunc)
        {}

        void StateFunc(TAutoPtr<IEventHandle>&) {}
    };

    struct TBindingObservation {
        THashSet<ui32> MobileNodes;
        bool SawCompleteMobileBinding = false;
        bool SawScatter = false;

        TBindingObservation() = default;

        explicit TBindingObservation(std::initializer_list<ui32> mobileNodes)
            : MobileNodes(mobileNodes.begin(), mobileNodes.end())
        {}

        bool ShouldRejectBinding(ui32 senderNodeId, ui32 recipientNodeId,
                                 const NKikimrBlobStorage::TEvNodeConfigPush& record) {
            const bool senderIsMobile = MobileNodes.contains(senderNodeId);
            const bool recipientIsMobile = MobileNodes.contains(recipientNodeId);
            if (senderIsMobile == recipientIsMobile) {
                return false;
            }

            if (!senderIsMobile) {
                return true;
            }

            THashSet<ui32> boundNodeIds;
            for (const auto& node : record.GetBoundNodes()) {
                boundNodeIds.insert(node.GetNodeId().GetNodeId());
            }
            if (std::ranges::all_of(MobileNodes, [&](ui32 nodeId) { return boundNodeIds.contains(nodeId); })) {
                SawCompleteMobileBinding = true;
                return false;
            }
            return true;
        }
    };

    class TNodeWardenProxyActor : public TActor<TNodeWardenProxyActor> {
        const TActorId KeeperId;
        TBindingObservation& Observation;

        void RejectBinding(const IEventHandle& request) {
            auto response = NStorage::TEvNodeConfigReversePush::MakeRejected();
            auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(request.Sender.NodeId()), SelfId(),
                                                         response.release(), 0, request.Cookie);
            handle->Rewrite(TEvInterconnect::EvForward, request.InterconnectSession);
            TActivationContext::Send(handle.release());
        }

    public:
        TNodeWardenProxyActor(TActorId keeperId, TBindingObservation& observation)
            : TActor(&TThis::StateFunc)
            , KeeperId(keeperId)
            , Observation(observation)
        {}

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (!ev->InterconnectSession) {
                if (type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                    Send(ev->Sender, new NStorage::TEvNodeWardenReadMetadataResult(
                        std::nullopt, NPDisk::EPDiskMetadataOutcome::NO_METADATA, {}), 0, ev->Cookie);
                }
                return;
            }

            if (type == NStorage::TEvNodeConfigPush::EventType) {
                const auto *msg = ev->Get<NStorage::TEvNodeConfigPush>();
                if (msg->Record.GetInitial() && Observation.ShouldRejectBinding(ev->Sender.NodeId(), SelfId().NodeId(), msg->Record)) {
                    RejectBinding(*ev);
                    return;
                }
            } else if (type == NStorage::TEvNodeConfigScatter::EventType) {
                Observation.SawScatter = true;
            }

            ev->Rewrite(type, KeeperId);
            TActivationContext::Send(ev.Release());
        }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDistconfNodeRoleTest) {
    NKikimrBlobStorage::TStorageConfig MakeStorageConfig(ui32 nodeCount, ui64 generation = 1,
                                                         const std::vector<ui32>& groupNodes = {},
                                                         i32 erasureSpecies = TBlobStorageGroupType::ErasureNone) {
        NKikimrBlobStorage::TStorageConfig config;
        config.SetGeneration(generation);
        AddStorageConfigNodes(config, nodeCount);

        auto *bsConfig = config.MutableBlobStorageConfig();
        bsConfig->MutableServiceSet();
        for (ui32 nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            auto *hostConfig = bsConfig->AddDefineHostConfig();
            hostConfig->SetHostConfigId(nodeId);
            auto *drive = hostConfig->AddDrive();
            drive->SetPath("/dev/disk" + std::to_string(nodeId) + "_1");
            drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);

            auto *host = bsConfig->MutableDefineBox()->AddHost();
            host->SetHostConfigId(nodeId);
            host->SetEnforcedNodeId(nodeId);
        }

        if (!groupNodes.empty()) {
            AddMultiVDiskGroup(config, groupNodes, erasureSpecies);
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        return config;
    }

    NKikimrBlobStorage::TStorageConfig MakeInitialStorageConfig(ui32 nodeCount) {
        NKikimrBlobStorage::TStorageConfig config;
        AddStorageConfigNodes(config, nodeCount);
        config.MutableBlobStorageConfig();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        return config;
    }

    bool HasQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::initializer_list<ui32> nodeIds) {
        std::vector<NStorage::TNodeIdentifier> successfulNodes;
        successfulNodes.reserve(nodeIds.size());
        for (ui32 nodeId : nodeIds) {
            UNIT_ASSERT(1 <= nodeId && nodeId <= static_cast<ui32>(config.AllNodesSize()));
            successfulNodes.emplace_back(config.GetAllNodes(nodeId - 1));
        }

        const auto nodeWardenConfig = MakeNodeWardenConfig(config);
        const THashMap<TString, TBridgePileId> bridgePileNameMap;
        return NStorage::HasNodeQuorum(config, successfulNodes, bridgePileNameMap, TBridgePileId(),
                                       *nodeWardenConfig, nullptr, true);
    }

    TActorId RegisterKeeper(TTestActorSystem& runtime, const NKikimrBlobStorage::TStorageConfig& config, ui32 nodeId,
                            TBindingObservation& observation) {
        auto nodeWardenConfig = MakeNodeWardenConfig(config);
        auto storageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(config);
        const TActorId keeperId = runtime.Register(
            new NStorage::TDistributedConfigKeeper(std::move(nodeWardenConfig), storageConfig, true), nodeId);

        const TActorId nodeWardenProxyId = runtime.Register(new TNodeWardenProxyActor(keeperId, observation), nodeId);
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), nodeWardenProxyId);

        const TActorId nameserviceId = runtime.Register(new TIgnoreActor, nodeId);
        runtime.RegisterService(GetNameserviceActorId(), nameserviceId);
        return keeperId;
    }

    std::optional<ui32> FindConvergedRoot(TTestActorSystem& runtime, const std::vector<TActorId>& keeperIds) {
        std::optional<ui32> rootNodeId;
        for (const TActorId keeperId : keeperIds) {
            ui32 reportedRootNodeId = 0;
            bool partOfNodeQuorum = false;
            const bool found = runtime.WrapInActorContext(keeperId, [&](IActor *actor) {
                auto *keeper = dynamic_cast<NStorage::TDistributedConfigKeeper*>(actor);
                UNIT_ASSERT(keeper);
                reportedRootNodeId = keeper->GetRootNodeId();
                partOfNodeQuorum = keeper->PartOfNodeQuorum();
            });
            UNIT_ASSERT(found);

            if (!partOfNodeQuorum) {
                return std::nullopt;
            }
            if (!rootNodeId) {
                rootNodeId = reportedRootNodeId;
            } else if (*rootNodeId != reportedRootNodeId) {
                return std::nullopt;
            }
        }
        return rootNodeId;
    }

    ui32 RunUntilConverged(const NKikimrBlobStorage::TStorageConfig& config, TBindingObservation& observation,
                           TDuration timeout = TDuration::Seconds(30), bool waitForScatter = false) {
        TTestActorSystem runtime(config.AllNodesSize());
        runtime.Start();
        Y_DEFER {
            runtime.Stop();
        };

        std::vector<TActorId> keeperIds;
        for (ui32 nodeId = 1; nodeId <= static_cast<ui32>(config.AllNodesSize()); ++nodeId) {
            keeperIds.push_back(RegisterKeeper(runtime, config, nodeId, observation));
        }

        const TInstant deadline = runtime.GetClock() + timeout;
        runtime.Schedule(deadline, nullptr, new TFakeSchedulerCookie, 1);

        std::optional<ui32> rootNodeId;
        runtime.Sim([&] {
            rootNodeId = FindConvergedRoot(runtime, keeperIds);
            const bool done = rootNodeId && (!waitForScatter || observation.SawScatter);
            return !done && runtime.GetClock() < deadline;
        });
        UNIT_ASSERT_C(rootNodeId && (!waitForScatter || observation.SawScatter),
                      "binding tree did not converge before the deadline");
        return *rootNodeId;
    }

    Y_UNIT_TEST(InitialConfigBootstrapUsesNodeMajority) {
        const auto config = MakeInitialStorageConfig(3);
        for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
            UNIT_ASSERT(!HasQuorum(config, {nodeId}));
        }
        UNIT_ASSERT(HasQuorum(config, {1, 2}));
    }

    Y_UNIT_TEST(InitialConfigConvergesToSingleRootThroughMailbox) {
        const auto config = MakeInitialStorageConfig(3);
        TBindingObservation observation;
        const ui32 rootNodeId = RunUntilConverged(config, observation, TDuration::Seconds(30), true);
        UNIT_ASSERT(observation.SawScatter);
        UNIT_ASSERT(1 <= rootNodeId && rootNodeId <= 3);
    }

    Y_UNIT_TEST(StaticGroupQuorumOverridesNodeMajority) {
        const auto config = MakeStorageConfig(3, 0, {1});
        UNIT_ASSERT(HasQuorum(config, {1}));
        UNIT_ASSERT(!HasQuorum(config, {2, 3}));
    }

    Y_UNIT_TEST(ConfigDriveQuorum) {
        const auto config = MakeStorageConfig(3, 1);
        UNIT_ASSERT(!HasQuorum(config, {1}));
        UNIT_ASSERT(HasQuorum(config, {1, 2}));
    }

    Y_UNIT_TEST(ThreeNodeSplitConvergesThroughMailbox) {
        const auto config = MakeStorageConfig(3, 0, {1});
        TBindingObservation observation{2, 3};
        const ui32 rootNodeId = RunUntilConverged(config, observation);
        UNIT_ASSERT(observation.SawCompleteMobileBinding);
        UNIT_ASSERT_VALUES_EQUAL(rootNodeId, 1u);
    }

    Y_UNIT_TEST(Block42QuorumOverridesNodeMajority) {
        const auto config = MakeStorageConfig(17, 0, {1, 2, 3, 4, 5, 6, 7, 8},
                                              TBlobStorageGroupType::Erasure4Plus2Block);
        UNIT_ASSERT(HasQuorum(config, {1, 2, 3, 4, 5, 6, 7, 8}));
        UNIT_ASSERT(!HasQuorum(config, {9, 10, 11, 12, 13, 14, 15, 16, 17}));
    }

    Y_UNIT_TEST(Block42SplitConvergesThroughMailbox) {
        const auto config = MakeStorageConfig(17, 0, {1, 2, 3, 4, 5, 6, 7, 8},
                                              TBlobStorageGroupType::Erasure4Plus2Block);
        TBindingObservation observation{9, 10, 11, 12, 13, 14, 15, 16, 17};
        const ui32 rootNodeId = RunUntilConverged(config, observation, TDuration::Minutes(10), true);
        UNIT_ASSERT(observation.SawCompleteMobileBinding);
        UNIT_ASSERT(observation.SawScatter);
        UNIT_ASSERT(1 <= rootNodeId && rootNodeId <= 8);
    }
}

Y_UNIT_TEST_SUITE(TDistconfGenerateConfigTest) {

    Y_UNIT_TEST(StaticGroupReassignParamsPreserveCommandAndTopology) {
        NKikimrBlobStorage::TStorageConfig config;
        NKikimrBlobStorage::TBaseConfig baseConfig;
        NKikimrBlobStorage::TNodeWardenServiceSet serviceSet;
        NKikimrBlobStorage::TGroupInfo group;
        group.SetGroupID(0);
        group.SetGroupGeneration(1);
        group.SetErasureSpecies(TBlobStorageGroupType::ErasureNone);

        const TVDiskID vdiskId(TGroupId::Zero(), 1, 0, 0, 0);
        auto *currentVDisk = serviceSet.AddVDisks();
        VDiskIDFromVDiskID(vdiskId, currentVDisk->MutableVDiskID());
        currentVDisk->MutableVDiskLocation()->SetNodeID(1);
        currentVDisk->MutableVDiskLocation()->SetPDiskID(1);

        auto *donorVDisk = serviceSet.AddVDisks();
        VDiskIDFromVDiskID(TVDiskID(TGroupId::Zero(), 0, 0, 0, 0), donorVDisk->MutableVDiskID());
        donorVDisk->MutableVDiskLocation()->SetNodeID(9);
        donorVDisk->MutableVDiskLocation()->SetPDiskID(3);
        donorVDisk->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);

        NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TReassignGroupDisk command;
        VDiskIDFromVDiskID(vdiskId, command.MutableVDiskId());
        command.MutablePDiskId()->SetNodeId(2);
        command.MutablePDiskId()->SetPDiskId(4);
        command.SetConvertToDonor(true);
        command.SetAllowUnusableDisks(true);
        command.SetIsSelfHealReasonDecommit(true);
        command.SetWithAttentionToReplication(true);
        command.SetTryToRelocateBrokenDisksLocallyFirst(true);
        command.SetFromSelfHeal(true);

        NKikimr::NStorage::TDistributedConfigKeeper::TStaticGroupReassignments reassignments;
        auto params = NStorage::TDistributedConfigKeeper::BuildStaticGroupReassignParams(&config, &baseConfig, command,
                                                                                         group, serviceSet, &reassignments);

        UNIT_ASSERT_VALUES_EQUAL(params.Config, &config);
        UNIT_ASSERT_VALUES_EQUAL(params.BaseConfig, &baseConfig);
        UNIT_ASSERT_VALUES_EQUAL(params.GroupId, TGroupId::Zero());
        UNIT_ASSERT_VALUES_EQUAL(params.GroupGeneration, 2u);
        UNIT_ASSERT_VALUES_EQUAL(params.GroupType.GetErasure(), TBlobStorageGroupType::ErasureNone);
        UNIT_ASSERT_VALUES_EQUAL(params.ReplacedDisks.at(TVDiskIdShort(vdiskId)), NBsController::TPDiskId(2, 4));
        UNIT_ASSERT(params.ForbiddenPDisks.contains(NBsController::TPDiskId(9, 3)));
        UNIT_ASSERT(params.ConvertToDonor);
        UNIT_ASSERT(!params.IgnoreVSlotQuotaCheck);
        UNIT_ASSERT(params.AllowUnusableDisks);
        UNIT_ASSERT(!params.SettleOnlyOnOperationalDisks);
        UNIT_ASSERT(params.IsSelfHealReasonDecommit);
        UNIT_ASSERT(!params.PreferLessOccupiedRack);
        UNIT_ASSERT(params.WithAttentionToReplication);
        UNIT_ASSERT(!params.UseSelfHealLocalPolicy);
        UNIT_ASSERT(params.TryToRelocateBrokenDisksLocallyFirst);
        UNIT_ASSERT(params.ApplySelfHealNodeAllowList);
        UNIT_ASSERT_VALUES_EQUAL(params.Reassignments, &reassignments);
    }

    Y_UNIT_TEST(AllocateStaticGroupTargetSpaceCheck) {
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);

        NKikimrBlobStorage::TStorageConfig config;
        auto *node = config.AddAllNodes();
        node->SetNodeId(1);
        node->MutableLocation()->SetDataCenter("dc-1");
        node->MutableLocation()->SetRack("rack-1");
        node->MutableLocation()->SetUnit("unit-1");

        NKikimrBlobStorage::TBaseConfig baseConfig;
        baseConfig.MutableSettings()->AddDefaultMaxSlots(16);

        auto *pdisk = baseConfig.AddPDisk();
        pdisk->SetNodeId(1);
        pdisk->SetPDiskId(1);
        pdisk->SetPath("/dev/disk1");
        pdisk->SetType(NKikimrBlobStorage::SSD);
        pdisk->SetKind(0);
        pdisk->SetGuid(1);
        pdisk->SetDriveStatus(NKikimrBlobStorage::ACTIVE);
        pdisk->SetDecommitStatus(NKikimrBlobStorage::DECOMMIT_NONE);
        pdisk->SetExpectedSlotCount(4);
        pdisk->SetExpectedSlotSize(100);
        pdisk->MutablePDiskConfig()->SetExpectedSlotSize(100);
        pdisk->MutablePDiskConfig()->SetMaxSlots(4);
        pdisk->MutablePDiskMetrics()->SetTotalSize(1000);
        pdisk->MutablePDiskMetrics()->SetAvailableSize(1000);

        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(1);
        geometry.SetNumFailDomainsPerFailRealm(1);
        geometry.SetNumVDisksPerFailDomain(1);
        auto *selfManagementConfig = config.MutableSelfManagementConfig();
        selfManagementConfig->MutableGeometry()->CopyFrom(geometry);
        selfManagementConfig->SetPDiskType(NKikimrBlobStorage::SSD);

        NKikimrBlobStorage::TStorageConfig configWithIgnoredCheck = config;

        try {
            keeper.AllocateStaticGroup({
                .Config = &config,
                .GroupId = TGroupId::Zero(),
                .GroupGeneration = 1,
                .GroupType = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
                .RequiredSpace = 200,
                .BaseConfig = &baseConfig,
            });
            UNIT_FAIL("Expected group allocation to fail");
        } catch (const NStorage::TDistributedConfigKeeper::TExConfigError& ex) {
            const TString error = ex.what();
            UNIT_ASSERT_C(error.Contains("group allocation failed"), error);
            UNIT_ASSERT_C(error.Contains("-v"), error);
        }

        UNIT_ASSERT_NO_EXCEPTION(keeper.AllocateStaticGroup({
            .Config = &configWithIgnoredCheck,
            .GroupId = TGroupId::Zero(),
            .GroupGeneration = 1,
            .GroupType = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
            .RequiredSpace = 200,
            .BaseConfig = &baseConfig,
            .IgnoreVSlotQuotaCheck = true,
        }));
    }

    Y_UNIT_TEST(AllocateStaticGroupOnFreshDrivesWithExpectedSlotSize) {
        // Bootstrap self-assembly: the static group must be allocatable on drives that come
        // straight from the config with expected_slot_size + max_slots, when neither the
        // materialized ExpectedSlotCount nor PDisk metrics exist yet. MaxSlots serves as the
        // slot count upper bound until NodeWarden computes the real value from the drive size.
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);

        NKikimrBlobStorage::TStorageConfig config;
        auto *node = config.AddAllNodes();
        node->SetNodeId(1);
        node->MutableLocation()->SetDataCenter("dc-1");
        node->MutableLocation()->SetRack("rack-1");
        node->MutableLocation()->SetUnit("unit-1");

        auto *bsConfig = config.MutableBlobStorageConfig();
        auto *hostConfig = bsConfig->AddDefineHostConfig();
        hostConfig->SetHostConfigId(1);
        auto *drive = hostConfig->AddDrive();
        drive->SetPath("/dev/disk1");
        drive->SetType(NKikimrBlobStorage::SSD);
        drive->MutablePDiskConfig()->SetExpectedSlotSize(100ull << 30);
        drive->MutablePDiskConfig()->SetMaxSlots(4);

        auto *host = bsConfig->MutableDefineBox()->AddHost();
        host->SetHostConfigId(1);
        host->SetEnforcedNodeId(1);

        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(1);
        geometry.SetNumFailDomainsPerFailRealm(1);
        geometry.SetNumVDisksPerFailDomain(1);
        auto *selfManagementConfig = config.MutableSelfManagementConfig();
        selfManagementConfig->MutableGeometry()->CopyFrom(geometry);
        selfManagementConfig->SetPDiskType(NKikimrBlobStorage::SSD);

        keeper.AllocateStaticGroup({
            .Config = &config,
            .GroupId = TGroupId::Zero(),
            .GroupGeneration = 1,
            .GroupType = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
        });

        const auto& serviceSet = config.GetBlobStorageConfig().GetServiceSet();
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.PDisksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GetPDisks(0).GetPath(), "/dev/disk1");
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GetPDisks(0).GetPDiskConfig().GetExpectedSlotSize(), 100ull << 30);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GetPDisks(0).GetPDiskConfig().GetMaxSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.VDisksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GroupsSize(), 1);
    }

    NKikimrConfig::TStateStorageConfig GenerateSimpleStateStorage(ui32 nodes, std::unordered_set<ui32> usedNodes = {},
            ui32 overrideReplicasInRingCount = 0, ui32 overrideRingsCount = 0, ui32 replicasSpecificVolume = 200,
            std::unordered_set<ui32> nodesToUse = {}, bool *goodConfigOut = nullptr,
            const NKikimrConfig::TStateStorageConfig& oldSS = {}, bool automaticManagement = true) {
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        NKikimrConfig::TStateStorageConfig ss;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(nodes)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        bool goodConfig = keeper.GenerateStateStorageConfig(&ss, config, usedNodes, nodesToUse, &oldSS,
            automaticManagement, overrideReplicasInRingCount, overrideRingsCount, replicasSpecificVolume);
        if (goodConfigOut) {
            *goodConfigOut = goodConfig;
        }
        return ss;
    }

    NKikimrConfig::TStateStorageConfig GenerateDCStateStorage(
        ui32 dcCnt
        , ui32 racksCnt
        , ui32 nodesInRack
        , std::unordered_map<ui32, ui32> nodesState = {}
        , std::unordered_set<ui32> usedNodes = {}
        , std::vector<ui32> oldConfig = {}
        , ui32 oldNToSelect = 9
        , ui32 overrideReplicasInRingCount = 0
        , ui32 overrideRingsCount = 0
        , ui32 replicasSpecificVolume = 1000
        , std::unordered_set<ui32> nodesToUse = {}
        , bool *goodConfigOut = nullptr
        , bool automaticManagement = true
    ) {
        NKikimrBlobStorage::TStorageConfig config;
        ui32 nodeId = 1;
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        for (ui32 dc : xrange(dcCnt)) {
            for (ui32 rack : xrange(racksCnt)) {
                for (auto _ : xrange(nodesInRack)) {
                    auto *node = config.AddAllNodes();
                    keeper.SelfHealNodesState[nodeId] = 0;
                    node->SetNodeId(nodeId++);
                    node->MutableLocation()->SetDataCenter("dc-" + std::to_string(dc));
                    node->MutableLocation()->SetRack(std::to_string(rack));
                }
            }
        }
        NKikimrConfig::TStateStorageConfig oldSS;
        if (!oldConfig.empty()) {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(oldNToSelect);
            for (ui32 node : oldConfig) {
                auto* ssRing = rg->AddRing();
                ssRing->AddNode(node);
            }
        }
        NKikimrConfig::TStateStorageConfig ss;
        for (auto [nodeId, state] : nodesState) {
            keeper.SelfHealNodesState[nodeId] = state;
        }
        bool goodConfig = keeper.GenerateStateStorageConfig(&ss, config, usedNodes, nodesToUse, &oldSS,
            automaticManagement, overrideReplicasInRingCount, overrideRingsCount, replicasSpecificVolume);
        if (goodConfigOut) {
            *goodConfigOut = goodConfig;
        }
        return ss;
    }

    void CheckStateStorage(const NKikimrConfig::TStateStorageConfig& ss, ui32 nToSelect, const std::unordered_set<ui32>& nodes) {
        auto &rg = ss.GetRingGroups(0);
        Cerr << "Actual: " << ss << " Expected: NToSelect: " << nToSelect << Endl;
        UNIT_ASSERT_EQUAL(rg.GetNToSelect(), nToSelect);
        UNIT_ASSERT_EQUAL(rg.RingSize(), nodes.size());
        std::unordered_set<ui32> usedNodes;
        for (ui32 i : xrange(nodes.size())) {
            UNIT_ASSERT_EQUAL(rg.GetRing(i).NodeSize(), 1);
            auto n = rg.GetRing(i).GetNode(0);
            UNIT_ASSERT(nodes.contains(n));
            UNIT_ASSERT(usedNodes.insert(n).second);
        }
    }

    Y_UNIT_TEST(GenerateConfigSimpleCases) {
        CheckStateStorage(GenerateSimpleStateStorage(1), 1, {1});
        CheckStateStorage(GenerateSimpleStateStorage(2), 1, {1, 2});
        CheckStateStorage(GenerateSimpleStateStorage(3), 3, {1, 2, 3});
        CheckStateStorage(GenerateSimpleStateStorage(8), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateSimpleStateStorage(9), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateDCStateStorage(1, 10, 5), 5, {1, 6, 11, 16, 21, 26, 31, 36});
    }

    Y_UNIT_TEST(GenerateConfig3DCCases) {
        CheckStateStorage(GenerateDCStateStorage(3, 1, 1), 3, {1, 2, 3});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 2), 3, {1, 3, 5});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 3), 9, {1, 2, 3, 4, 5, 6, 7, 8, 9});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 18), 9, {1, 2, 3, 19, 20, 21, 37, 38, 39});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
    }

    void CheckStateStorage2(const NKikimrConfig::TStateStorageConfig& ss, std::string expected) {
        TString actual = TStringBuilder() << ss;
        if (actual != expected) {
            Cerr << "Err Actual: " << ss << Endl;
            Cerr << "Err Expected: " << expected << Endl;
        }
        UNIT_ASSERT_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(GenerateConfig1DCBigCases) {
        CheckStateStorage2(GenerateDCStateStorage(1, 1, 1000), "{ RingGroups { NToSelect: 5 "
            "Ring { Node: 1 Node: 2 } Ring { Node: 3 Node: 4 } Ring { Node: 5 Node: 6 } "
            "Ring { Node: 7 Node: 8 } Ring { Node: 9 Node: 10 } Ring { Node: 11 Node: 12 } "
            "Ring { Node: 13 Node: 14 } Ring { Node: 15 Node: 16 } } }");
        CheckStateStorage2(GenerateDCStateStorage(1, 2, 1000), "{ RingGroups { NToSelect: 5 "
            "Ring { Node: 1 Node: 2 Node: 3 } Ring { Node: 4 Node: 5 Node: 6 } Ring { Node: 7 Node: 8 Node: 9 } "
            "Ring { Node: 10 Node: 11 Node: 12 } Ring { Node: 13 Node: 14 Node: 15 } Ring { Node: 16 Node: 17 Node: 18 } "
            "Ring { Node: 19 Node: 20 Node: 21 } Ring { Node: 22 Node: 23 Node: 24 } } }");
        CheckStateStorage2(GenerateDCStateStorage(1, 10, 100), "{ RingGroups { NToSelect: 5 "
            "Ring { Node: 1 Node: 2 } Ring { Node: 101 Node: 102 } Ring { Node: 201 Node: 202 } "
            "Ring { Node: 301 Node: 302 } Ring { Node: 401 Node: 402 } Ring { Node: 501 Node: 502 } "
            "Ring { Node: 601 Node: 602 } Ring { Node: 701 Node: 702 } } }");
    }

    Y_UNIT_TEST(GenerateConfig3DCBigCases) {
        CheckStateStorage2(GenerateDCStateStorage(3, 1, 300), "{ RingGroups { NToSelect: 9 "
            "Ring { Node: 1 } Ring { Node: 2 } Ring { Node: 3 } "
            "Ring { Node: 301 } Ring { Node: 302 } Ring { Node: 303 } "
            "Ring { Node: 601 } Ring { Node: 602 } Ring { Node: 603 } } }");
        CheckStateStorage2(GenerateDCStateStorage(3, 10, 100), "{ RingGroups { NToSelect: 9 "
            "Ring { Node: 1 Node: 2 Node: 3 Node: 4 } Ring { Node: 101 Node: 102 Node: 103 Node: 104 } "
            "Ring { Node: 201 Node: 202 Node: 203 Node: 204 } Ring { Node: 1001 Node: 1002 Node: 1003 Node: 1004 } "
            "Ring { Node: 1101 Node: 1102 Node: 1103 Node: 1104 } Ring { Node: 1201 Node: 1202 Node: 1203 Node: 1204 } "
            "Ring { Node: 2001 Node: 2002 Node: 2003 Node: 2004 } Ring { Node: 2101 Node: 2102 Node: 2103 Node: 2104 } "
            "Ring { Node: 2201 Node: 2202 Node: 2203 Node: 2204 } } }");
    }

    Y_UNIT_TEST(IgnoreNodes) {
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20, { {3, 2} }), 5, {1, 2, 4, 5, 6, 7, 8, 9});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20, { {3, 2}, {7, 4}, {10, 3} }), 5, {1, 2, 4, 5, 6, 8, 9, 11});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 10, { {3, 2}, {7, 4}, {10, 3} }), 5, {1, 2, 3, 4, 5, 6, 8, 9});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 10, { {3, 3}, {7, 4}, {10, 2} }), 5, {1, 2, 4, 5, 6, 8, 9, 10});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2} }), 9, {1, 4, 7, 10, 14, 16, 19, 22, 25});
    }

    Y_UNIT_TEST(BadRack) {
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 5}, {14, 3}, {15, 4} }), 9, {1, 4, 7, 10, 14, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2}, {14, 3}, {15, 4} }), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 3}, {14, 4}, {15, 2} }), 9, {1, 4, 7, 10, 15, 16, 19, 22, 25});
    }

    Y_UNIT_TEST(ExtraDCHelp) {
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {3, 2} }), 9, {1, 2, 4, 5, 6, 7, 8, 9, 10});
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {6, 2} }), 9, {1, 2, 3, 4, 5, 7, 8, 9, 10});
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {9, 2}, {8, 4} }), 9, {1, 2, 3, 4, 5, 6, 7, 10, 11});
    }


    Y_UNIT_TEST(UsedNodes) {
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2} }, { 1, 2, 3, 4, 5, 6 }), 9, {1, 4, 7, 10, 14, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20, { {3, 2} }, { 1, 2, 3, 4, 9 }), 5, {5, 6, 7, 8, 10, 11, 12, 13});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2} }, { 4, 16 }), 9, {1, 5, 7, 10, 14, 17, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {3, 2} }, { 1 }), 9, {2, 4, 5, 6, 7, 8, 9, 10, 11});
    }


    Y_UNIT_TEST(UseOldNodesInDisconnectedDC) {
        // DC is connected, not enough bad nodes in DC - normak config generation
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {10, 2}, {11, 4}, {13, 3}, {14, 4} }, {}, {1, 5, 8, 10, 14, 17, 19, 22, 25}), 9, {1, 4, 7, 12, 15, 16, 19, 22, 25});
        // Disconnected DC, but current config is invalid, build new config without usage of node statuses in disconnected DC
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {10, 2}, {11, 4}, {13, 3}, {14, 4}, {15, 2} }, {}, {1, 5, 8, 10, 14, 17, 19, 22, 25}, 5), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
        // DC disconnected - use previous config for this DC
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {10, 2}, {11, 4}, {13, 3}, {14, 4}, {15, 2} }, {}, {1, 5, 8, 10, 14, 17, 19, 22, 25}, 9), 9, {1, 4, 7, 10, 14, 17, 19, 22, 25});
    }

    Y_UNIT_TEST(GenerateConfigReplicasOverrides) {
        CheckStateStorage(GenerateSimpleStateStorage(100, {}, 0, 0), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateSimpleStateStorage(100, {}, 1, 1), 1, {1});
        CheckStateStorage2(GenerateSimpleStateStorage(100, {}, 3, 3),
            "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 Node: 3 }"
            " Ring { Node: 4 Node: 5 Node: 6 } Ring { Node: 7 Node: 8 Node: 9 } } }");
        CheckStateStorage(GenerateSimpleStateStorage(100, {}, 20, 10), 5, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        CheckStateStorage2(GenerateSimpleStateStorage(16, {}, 4, 2), "{ RingGroups { NToSelect: 1 Ring { Node: 1 Node: 2 Node: 3 Node: 4 } Ring { Node: 5 Node: 6 Node: 7 Node: 8 } } }");
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 0, 0), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 1, 3), 3, {1, 10, 19});
        CheckStateStorage2(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 2, 3), "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 } Ring { Node: 10 Node: 11 } Ring { Node: 19 Node: 20 } } }");
        CheckStateStorage2(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 3, 3), "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 Node: 3 } Ring { Node: 10 Node: 11 Node: 12 } Ring { Node: 19 Node: 20 Node: 21 } } }");
        CheckStateStorage2(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 2, 1),"{ RingGroups { NToSelect: 9 "
            "Ring { Node: 1 Node: 2 } Ring { Node: 4 Node: 5 } Ring { Node: 7 Node: 8 } "
            "Ring { Node: 10 Node: 11 } Ring { Node: 13 Node: 14 } Ring { Node: 16 Node: 17 } "
            "Ring { Node: 19 Node: 20 } Ring { Node: 22 Node: 23 } Ring { Node: 25 Node: 26 } } }");
    }

    Y_UNIT_TEST(GenerateConfigReplicasSpecificVolume) {
        CheckStateStorage2(GenerateSimpleStateStorage(100, {}, 0, 3, 35),
            "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 Node: 3 }"
            " Ring { Node: 4 Node: 5 Node: 6 } Ring { Node: 7 Node: 8 Node: 9 } } }");
        CheckStateStorage2(GenerateSimpleStateStorage(16, {}, 0, 2, 5), "{ RingGroups { NToSelect: 1 Ring { Node: 1 Node: 2 Node: 3 Node: 4 } Ring { Node: 5 Node: 6 Node: 7 Node: 8 } } }");
    }

    Y_UNIT_TEST(NodesToUseEmptyMeansNoRestriction) {
        // Explicit regression check: passing an empty nodesToUse set behaves exactly
        // like not passing the parameter at all (default argument in other tests).
        CheckStateStorage(GenerateSimpleStateStorage(8, {}, 0, 0, 200, {}), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 1, {}, {}, {}, 9, 0, 0, 1000, {}), 3, {1, 2, 3});
    }

    Y_UNIT_TEST(NodesToUseRestrictsSimplePool) {
        // Out of 10 available nodes, only {5, 6, 7} are allowed to be used.
        CheckStateStorage(GenerateSimpleStateStorage(10, {}, 0, 0, 200, {5, 6, 7}), 3, {5, 6, 7});
    }

    Y_UNIT_TEST(NodesToUseKeepsOriginalNodeIds) {
        // Restrict a big pool of 100 nodes down to 8 specific nodes; the algorithm
        // should behave exactly as if only those 8 nodes existed, while preserving
        // their original (non-contiguous) node ids.
        CheckStateStorage(
            GenerateSimpleStateStorage(100, {}, 0, 0, 200, {10, 20, 30, 40, 50, 60, 70, 80}),
            5, {10, 20, 30, 40, 50, 60, 70, 80});
    }

    Y_UNIT_TEST(NodesToUseIgnoresUnknownNodeIds) {
        // Node ids in nodesToUse that are not present in baseConfig's AllNodes
        // must simply be ignored, not cause a crash or a bad config.
        CheckStateStorage(GenerateSimpleStateStorage(3, {}, 0, 0, 200, {1, 2, 3, 9999}), 3, {1, 2, 3});
    }

    Y_UNIT_TEST(NodesToUseAllUnknownYieldsBadConfigNotEmptySS) {
        // Regression test: if nodesToUse is non-empty, but *none* of the specified node ids
        // exist in baseConfig's AllNodes (e.g. StateStorageSelfHealAllowedNodes referencing
        // decommissioned nodes), the generator must not silently produce an empty (but "good")
        // state storage config, since that would trigger a destructive full reconfiguration
        // to an empty state storage in the self-heal path. Instead, GenerateStateStorageConfig
        // must report the config as bad.
        bool goodConfig = true;
        auto ss = GenerateSimpleStateStorage(3, {}, 0, 0, 200, {9998, 9999}, &goodConfig);
        UNIT_ASSERT(!goodConfig);
        UNIT_ASSERT_EQUAL(ss.RingGroupsSize(), 0);
    }

    Y_UNIT_TEST(NodesToUseAllUnknownInDCTopologyYieldsBadConfig) {
        // Same regression, but exercised through the multi-DC/rack topology generator, since
        // that is the code path used in practice by the self-heal state storage logic.
        bool goodConfig = true;
        auto ss = GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 0, 0, 1000, /*nodesToUse=*/{9998, 9999}, &goodConfig);
        UNIT_ASSERT(!goodConfig);
        UNIT_ASSERT_EQUAL(ss.RingGroupsSize(), 0);
    }

    Y_UNIT_TEST(NodesToUseForcesAlternateNodeSelectionInDC) {
        // Without restriction, the default pick within each 3-node rack is its
        // first node: {1, 4, 7, 10, 13, 16, 19, 22, 25}.
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});

        // Excluding those default nodes from nodesToUse forces the generator to
        // fall back to the next available node in each rack.
        std::unordered_set<ui32> nodesToUse = {
            2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 23, 24, 26, 27
        };
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 0, 0, 1000, nodesToUse),
            9, {2, 5, 8, 11, 14, 17, 20, 23, 26});
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Cases where the requested configuration (via overrideRingsCount/overrideReplicasInRingCount)
    // can not be honored because nodesToUse restricts the available nodes too much.

    Y_UNIT_TEST(NodesToUseInsufficientForOverrideRingsCountSingleGroup) {
        // Only 3 nodes are usable (all with a known GOOD self-heal state, via GenerateDCStateStorage),
        // but the caller explicitly asks for 8 rings: impossible to satisfy.
        bool goodConfig = true;
        GenerateDCStateStorage(1, 1, 100, {}, {}, {}, 9, /*overrideReplicasInRingCount=*/0, /*overrideRingsCount=*/8,
            1000, /*nodesToUse=*/{1, 2, 3}, &goodConfig);
        UNIT_ASSERT(!goodConfig);

        // Sanity check: with exactly as many nodes as requested rings, the override is satisfiable.
        bool goodConfig2 = false;
        GenerateDCStateStorage(1, 1, 100, {}, {}, {}, 9, 0, /*overrideRingsCount=*/3, 1000, /*nodesToUse=*/{1, 2, 3}, &goodConfig2);
        UNIT_ASSERT(goodConfig2);
    }

    Y_UNIT_TEST(NodesToUseInsufficientForOverrideReplicasInRingCountSingleGroup) {
        // Only 2 nodes are usable, but the caller explicitly asks for 3 replicas per ring:
        // impossible to satisfy (would need at least 3 nodes for a single ring).
        bool goodConfig = true;
        GenerateDCStateStorage(1, 1, 100, {}, {}, {}, 9, /*overrideReplicasInRingCount=*/3, /*overrideRingsCount=*/1,
            1000, /*nodesToUse=*/{1, 2}, &goodConfig);
        UNIT_ASSERT(!goodConfig);
    }

    Y_UNIT_TEST(NodesToUseInsufficientForOverrideRingsCountMultiGroup) {
        // 3-DC topology, but nodesToUse leaves only 2 nodes in one of the DCs while the
        // caller explicitly requests 9 rings (3 per group) - the smallest group can't supply 3 rings.
        std::unordered_set<ui32> nodesToUse = {
            1, 4, 7,             // dc-0: full rack representation (3 nodes)
            10, 13,              // dc-1: only 2 nodes available
            19, 22, 25,          // dc-2: full rack representation (3 nodes)
        };
        bool goodConfig = true;
        GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 0, /*overrideRingsCount=*/9, 1000, nodesToUse, &goodConfig);
        UNIT_ASSERT(!goodConfig);
    }

    Y_UNIT_TEST(NodesToUseSufficientForOverrideKeepsGoodConfig) {
        // Same restriction style as above, but this time nodesToUse leaves exactly enough
        // nodes in every group to satisfy the override - the config must be reported as good.
        std::unordered_set<ui32> nodesToUse = {
            1, 4, 7,
            10, 13, 16,
            19, 22, 25,
        };
        bool goodConfig = false;
        GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 0, /*overrideRingsCount=*/9, 1000, nodesToUse, &goodConfig);
        UNIT_ASSERT(goodConfig);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // AutomaticManagement (AutomaticStateStorageManagement / AutomaticStateStorageBoardManagement /
    // AutomaticSchemeBoardManagement) semantics at the GenerateStateStorageConfig level: when the
    // subsystem's automatic management is disabled, self-heal must keep the current (old) config
    // untouched and report the result as "good", regardless of node states or overrides.

    Y_UNIT_TEST(AutomaticManagementDisabledKeepsOldConfigUnchanged) {
        NKikimrConfig::TStateStorageConfig oldSS;
        {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(1);
            auto* ring = rg->AddRing();
            ring->AddNode(42);
        }

        bool goodConfig = false;
        auto ss = GenerateSimpleStateStorage(8, {}, 0, 0, 200, {}, &goodConfig, oldSS, /*automaticManagement=*/false);

        // The generator must not have touched node selection at all: result equals oldSS verbatim.
        UNIT_ASSERT_EQUAL(TStringBuilder() << ss, TStringBuilder() << oldSS);
        UNIT_ASSERT(goodConfig);
    }

    Y_UNIT_TEST(AutomaticManagementEnabledGeneratesNewConfig) {
        NKikimrConfig::TStateStorageConfig oldSS;
        {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(1);
            auto* ring = rg->AddRing();
            ring->AddNode(42);
        }

        bool goodConfig = false;
        auto ss = GenerateSimpleStateStorage(8, {}, 0, 0, 200, {}, &goodConfig, oldSS, /*automaticManagement=*/true);

        // With automatic management enabled (default), a fresh configuration must be generated
        // from the current node pool, ignoring the stale oldSS content.
        UNIT_ASSERT(TStringBuilder() << ss != TStringBuilder() << oldSS);
        CheckStateStorage(ss, 5, {1, 2, 3, 4, 5, 6, 7, 8});
    }

    Y_UNIT_TEST(AutomaticManagementDisabledIgnoresBadNodeStates) {
        // Even if nodesToUse/overrides would normally produce a bad config, disabling automatic
        // management must short-circuit generation entirely and still report "good", since the
        // old config is kept as-is and self-heal is not supposed to touch it.
        NKikimrConfig::TStateStorageConfig oldSS;
        {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(1);
            auto* ring = rg->AddRing();
            ring->AddNode(7);
        }
        bool goodConfig = false;
        GenerateSimpleStateStorage(100, {}, /*overrideReplicasInRingCount=*/0, /*overrideRingsCount=*/8,
            200, /*nodesToUse=*/{1, 2, 3}, &goodConfig, oldSS, /*automaticManagement=*/false);
        UNIT_ASSERT(goodConfig);
    }

    Y_UNIT_TEST(AutomaticManagementDisabledPopulatesUsedNodes) {
        // Regression test: when automaticManagement is false, the generator must still populate
        // usedNodes with the node IDs taken from oldConfig, because usedNodes is shared across
        // the StateStorage / StateStorageBoard / SchemeBoard generator invocations, and subsequent
        // subsystem generators rely on it to avoid co-locating replicas on the same nodes.
        NKikimrConfig::TStateStorageConfig oldSS;
        {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(2);
            auto* ring1 = rg->AddRing();
            ring1->AddNode(3);
            auto* ring2 = rg->AddRing();
            ring2->AddNode(5);
        }

        std::unordered_set<ui32> usedNodes;
        bool goodConfig = false;
        GenerateSimpleStateStorage(8, usedNodes, 0, 0, 200, {}, &goodConfig, oldSS,
            /*automaticManagement=*/false);
        UNIT_ASSERT(goodConfig);

        // usedNodes is passed by value into GenerateSimpleStateStorage's helper, so re-derive it
        // directly via the keeper API to check that the reference-passed set was actually filled.
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        NKikimrConfig::TStateStorageConfig ssOut;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(8)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        std::unordered_set<ui32> refUsedNodes;
        bool refGoodConfig = keeper.GenerateStateStorageConfig(&ssOut, config, refUsedNodes, {}, &oldSS,
            /*automaticManagement=*/false, 0, 0, 200);
        UNIT_ASSERT(refGoodConfig);
        UNIT_ASSERT(refUsedNodes.contains(3));
        UNIT_ASSERT(refUsedNodes.contains(5));
        UNIT_ASSERT_EQUAL(refUsedNodes.size(), 2u);
    }

    Y_UNIT_TEST(AutomaticManagementDisabledUsedNodesAvoidedBySubsequentGenerator) {
        // Simulate the real-world scenario from the bug report: StateStorage has automatic
        // management disabled (its nodes are kept as-is), while StateStorageBoard has it enabled.
        // The board generator must avoid nodes already occupied by StateStorage replicas, which
        // are threaded through the shared usedNodes set.
        NKikimrConfig::TStateStorageConfig oldStateStorage;
        {
            auto* rg = oldStateStorage.AddRingGroups();
            rg->SetNToSelect(3);
            for (ui32 node : {1, 2, 3}) {
                auto* ring = rg->AddRing();
                ring->AddNode(node);
            }
        }

        // Use a node pool large enough that plenty of spare nodes remain even after 3 are marked
        // as used by StateStorage, so the board generator has enough room to build its own config.
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(20)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }

        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        // Explicitly mark all nodes as GOOD; otherwise nodes absent from SelfHealNodesState are
        // treated as UNKNOWN, which would make IsGoodConfig() always return false.
        for (ui32 i : xrange(20)) {
            keeper.SelfHealNodesState[i + 1] = 0;
        }
        std::unordered_set<ui32> usedNodes;

        // Step 1: StateStorage generation with automatic management disabled.
        NKikimrConfig::TStateStorageConfig stateStorageOut;
        bool goodConfig1 = keeper.GenerateStateStorageConfig(&stateStorageOut, config, usedNodes, {},
            &oldStateStorage, /*automaticManagement=*/false, 0, 0, 200);
        UNIT_ASSERT(goodConfig1);
        UNIT_ASSERT(usedNodes.contains(1));
        UNIT_ASSERT(usedNodes.contains(2));
        UNIT_ASSERT(usedNodes.contains(3));

        // Step 2: StateStorageBoard generation with automatic management enabled, sharing usedNodes.
        NKikimrConfig::TStateStorageConfig boardOut;
        NKikimrConfig::TStateStorageConfig emptyOldBoard;
        bool goodConfig2 = keeper.GenerateStateStorageConfig(&boardOut, config, usedNodes, {},
            &emptyOldBoard, /*automaticManagement=*/true, 0, 0, 200);
        UNIT_ASSERT(goodConfig2);

        // None of the nodes selected for the board must overlap with the nodes already used by
        // StateStorage, since those were marked as used via usedNodes.
        for (const auto& rg : boardOut.GetRingGroups()) {
            for (const auto& ring : rg.GetRing()) {
                for (ui32 node : ring.GetNode()) {
                    UNIT_ASSERT_C(!(node == 1 || node == 2 || node == 3),
                        "StateStorageBoard replica placed on node " << node
                            << " which is already occupied by StateStorage");
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TDeriveStorageConfigCleanupTest) {

    struct TTestSetup {
        NKikimrBlobStorage::TStorageConfig StorageConfig;
        NKikimrConfig::TAppConfig AppConfig;

        void AddNode(ui32 nodeId, const TString& host = "host", ui16 port = 19000) {
            {
                auto* node = StorageConfig.AddAllNodes();
                node->SetNodeId(nodeId);
                node->SetHost(host);
                node->SetPort(port);
            }
            {
                auto* node = AppConfig.MutableNameserviceConfig()->AddNode();
                node->SetNodeId(nodeId);
                node->SetInterconnectHost(host);
                node->SetPort(port);
            }
        }

        void RemoveNodeFromAppConfig(ui32 nodeId) {
            auto* ns = AppConfig.MutableNameserviceConfig();
            auto* nodes = ns->MutableNode();
            for (int i = nodes->size() - 1; i >= 0; --i) {
                if (nodes->Get(i).GetNodeId() == nodeId) {
                    nodes->DeleteSubrange(i, 1);
                }
            }
        }

        void AddPDisk(ui32 nodeId, ui32 pdiskId, const TString& path = "/dev/disk") {
            auto* pdisk = StorageConfig.MutableBlobStorageConfig()->MutableServiceSet()->AddPDisks();
            pdisk->SetNodeID(nodeId);
            pdisk->SetPDiskID(pdiskId);
            pdisk->SetPath(path);
            pdisk->SetPDiskGuid(nodeId * 1000 + pdiskId);
            pdisk->SetPDiskCategory(1);
        }

        void AddVDisk(ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui32 groupId, ui32 generation,
                      ui32 domain, NKikimrBlobStorage::EEntityStatus status = NKikimrBlobStorage::EEntityStatus::INITIAL) {
            auto* vdisk = StorageConfig.MutableBlobStorageConfig()->MutableServiceSet()->AddVDisks();
            auto* vid = vdisk->MutableVDiskID();
            vid->SetGroupID(groupId);
            vid->SetGroupGeneration(generation);
            vid->SetRing(0);
            vid->SetDomain(domain);
            vid->SetVDisk(0);
            auto* loc = vdisk->MutableVDiskLocation();
            loc->SetNodeID(nodeId);
            loc->SetPDiskID(pdiskId);
            loc->SetVDiskSlotID(vslotId);
            loc->SetPDiskGuid(nodeId * 1000 + pdiskId);
            if (status != NKikimrBlobStorage::EEntityStatus::INITIAL) {
                vdisk->SetEntityStatus(status);
            }
        }

        void AddGroup(ui32 groupId, ui32 generation, const std::vector<std::tuple<ui32, ui32, ui32>>& vdiskLocs) {
            auto* group = StorageConfig.MutableBlobStorageConfig()->MutableServiceSet()->AddGroups();
            group->SetGroupID(groupId);
            group->SetGroupGeneration(generation);
            group->SetErasureSpecies(4);
            auto* ring = group->AddRings();
            for (const auto& [nodeId, pdiskId, vslotId] : vdiskLocs) {
                auto* fd = ring->AddFailDomains();
                auto* loc = fd->AddVDiskLocations();
                loc->SetNodeID(nodeId);
                loc->SetPDiskID(pdiskId);
                loc->SetVDiskSlotID(vslotId);
                loc->SetPDiskGuid(nodeId * 1000 + pdiskId);
            }
        }

        void EnableSelfManagement() {
            StorageConfig.MutableSelfManagementConfig()->SetEnabled(true);
            AppConfig.MutableSelfManagementConfig()->SetEnabled(true);
        }

        void SyncAppConfigBlobStorage() {
            AppConfig.MutableBlobStorageConfig()->MutableServiceSet();
        }

        void MirrorServiceSetIntoAppConfig() {
            AppConfig.MutableBlobStorageConfig()->MutableServiceSet()->CopyFrom(
                StorageConfig.GetBlobStorageConfig().GetServiceSet());
        }

        bool Derive(TString* error = nullptr) {
            TString err;
            if (!error) error = &err;
            return NKikimr::NStorage::DeriveStorageConfig(AppConfig, &StorageConfig, error);
        }

        void AddDefineBoxHost(ui32 nodeId, ui64 hostConfigId = 1) {
            auto* host = AppConfig.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
            host->SetEnforcedNodeId(nodeId);
            host->SetHostConfigId(hostConfigId);
            auto* host2 = StorageConfig.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
            host2->SetEnforcedNodeId(nodeId);
            host2->SetHostConfigId(hostConfigId);
        }

        int CountPDisks() const { return StorageConfig.GetBlobStorageConfig().GetServiceSet().PDisksSize(); }
        int CountVDisks() const { return StorageConfig.GetBlobStorageConfig().GetServiceSet().VDisksSize(); }
        int CountDefineBoxHosts() const {
            return StorageConfig.GetBlobStorageConfig().HasDefineBox()
                ? StorageConfig.GetBlobStorageConfig().GetDefineBox().HostSize() : 0;
        }

        bool HasPDiskOnNode(ui32 nodeId) const {
            for (const auto& p : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                if (p.GetNodeID() == nodeId) return true;
            }
            return false;
        }

        bool HasVDiskOnNode(ui32 nodeId) const {
            for (const auto& v : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                if (v.HasVDiskLocation() && v.GetVDiskLocation().GetNodeID() == nodeId) return true;
            }
            return false;
        }

        bool HasDefineBoxHostForNode(ui32 nodeId) const {
            if (!StorageConfig.GetBlobStorageConfig().HasDefineBox()) return false;
            for (const auto& host : StorageConfig.GetBlobStorageConfig().GetDefineBox().GetHost()) {
                if (host.GetEnforcedNodeId() == nodeId) return true;
            }
            return false;
        }
    };

    Y_UNIT_TEST(DestroyVDiskAndPDiskCleanedOnNodeRemoval) {
        TTestSetup s;
        s.EnableSelfManagement();
        for (ui32 i = 1; i <= 8; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddPDisk(i, 1, "/dev/disk" + std::to_string(i));
            s.AddDefineBoxHost(i);
        }

        s.AddVDisk(1, 1, 1, 0, 1, 0, NKikimrBlobStorage::EEntityStatus::DESTROY);
        for (ui32 i = 2; i <= 8; ++i) {
            s.AddVDisk(i, 1, 1, 0, 2, i - 1);
        }
        std::vector<std::tuple<ui32, ui32, ui32>> groupLocs;
        for (ui32 i = 2; i <= 8; ++i) {
            groupLocs.emplace_back(i, 1, 1);
        }
        s.AddGroup(0, 2, groupLocs);
        s.SyncAppConfigBlobStorage();

        s.RemoveNodeFromAppConfig(1);

        TString error;
        UNIT_ASSERT_C(s.Derive(&error), "DeriveStorageConfig failed: " << error);

        UNIT_ASSERT(!s.HasVDiskOnNode(1));
        UNIT_ASSERT(!s.HasPDiskOnNode(1));
        UNIT_ASSERT(!s.HasDefineBoxHostForNode(1));
        UNIT_ASSERT(s.HasPDiskOnNode(2));
        UNIT_ASSERT(s.HasVDiskOnNode(2));
        UNIT_ASSERT(s.HasDefineBoxHostForNode(2));
        UNIT_ASSERT_EQUAL(s.CountPDisks(), 7);
        UNIT_ASSERT_EQUAL(s.CountVDisks(), 7);
        UNIT_ASSERT_EQUAL(s.CountDefineBoxHosts(), 7);
    }

    Y_UNIT_TEST(PDiskKeptWhenReferencedByLiveVDiskOnRemovedNode) {
        TTestSetup s;
        s.EnableSelfManagement();
        for (ui32 i = 1; i <= 4; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddPDisk(i, 1, "/dev/disk" + std::to_string(i));
            s.AddVDisk(i, 1, 1, 0, 1, i - 1);
        }
        std::vector<std::tuple<ui32, ui32, ui32>> groupLocs;
        for (ui32 i = 1; i <= 4; ++i) {
            groupLocs.emplace_back(i, 1, 1);
        }
        s.AddGroup(0, 1, groupLocs);
        s.SyncAppConfigBlobStorage();
        s.RemoveNodeFromAppConfig(1);

        TString error;
        UNIT_ASSERT_C(s.Derive(&error), "DeriveStorageConfig failed: " << error);
        UNIT_ASSERT(s.HasVDiskOnNode(1));
        UNIT_ASSERT(s.HasPDiskOnNode(1));
    }

    Y_UNIT_TEST(NoCleanupWithoutSelfManagement) {
        TTestSetup s;
        for (ui32 i = 1; i <= 4; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddPDisk(i, 1, "/dev/disk" + std::to_string(i));
            s.AddDefineBoxHost(i);
        }

        s.AddVDisk(1, 1, 1, 0, 1, 0, NKikimrBlobStorage::EEntityStatus::DESTROY);
        for (ui32 i = 2; i <= 4; ++i) {
            s.AddVDisk(i, 1, 1, 0, 2, i - 1);
        }
        std::vector<std::tuple<ui32, ui32, ui32>> groupLocs;
        for (ui32 i = 2; i <= 4; ++i) {
            groupLocs.emplace_back(i, 1, 1);
        }
        s.AddGroup(0, 2, groupLocs);
        s.MirrorServiceSetIntoAppConfig();
        s.RemoveNodeFromAppConfig(1);

        TString error;
        UNIT_ASSERT_C(s.Derive(&error), "DeriveStorageConfig failed: " << error);

        UNIT_ASSERT(s.HasVDiskOnNode(1));
        UNIT_ASSERT(s.HasPDiskOnNode(1));
        UNIT_ASSERT(s.HasDefineBoxHostForNode(1));
    }
}

Y_UNIT_TEST_SUITE(TDistconfStaticGroupSelfHealTest) {

    struct TSetup {
        NKikimrBlobStorage::TStorageConfig Config;
        NKikimrBlobStorage::TBaseConfig BaseConfig;

        void AddNode(ui32 nodeId, const TString& host, ui32 port = 19001,
                     const TString& dataCenter = "", const TString& rack = "") {
            auto *node = Config.AddAllNodes();
            node->SetNodeId(nodeId);
            node->SetHost(host);
            node->SetPort(port);
            if (dataCenter || rack) {
                node->MutableLocation()->SetDataCenter(dataCenter);
                node->MutableLocation()->SetRack(rack);
            }

            auto *baseNode = BaseConfig.AddNode();
            baseNode->SetNodeId(nodeId);
            baseNode->SetConnected(true);
        }

        void AddCandidatePDisk(ui32 nodeId, ui32 pdiskId) {
            auto *pdisk = BaseConfig.AddPDisk();
            pdisk->SetNodeId(nodeId);
            pdisk->SetPDiskId(pdiskId);
            pdisk->SetPath("/dev/disk" + std::to_string(nodeId) + "_" + std::to_string(pdiskId));
            pdisk->SetGuid(nodeId * 1000 + pdiskId);
            pdisk->SetType(NKikimrBlobStorage::EPDiskType::ROT);
            pdisk->SetDriveStatus(NKikimrBlobStorage::EDriveStatus::ACTIVE);
            pdisk->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE);
        }

        void AddGroupOn(ui32 nodeId, ui32 pdiskId) {
            auto *ss = Config.MutableBlobStorageConfig()->MutableServiceSet();

            auto *pdisk = ss->AddPDisks();
            pdisk->SetNodeID(nodeId);
            pdisk->SetPDiskID(pdiskId);
            pdisk->SetPath("/dev/disk" + std::to_string(nodeId) + "_" + std::to_string(pdiskId));
            pdisk->SetPDiskGuid(nodeId * 1000 + pdiskId);
            pdisk->SetPDiskCategory(0);

            auto *vdisk = ss->AddVDisks();
            auto *vid = vdisk->MutableVDiskID();
            vid->SetGroupID(0);
            vid->SetGroupGeneration(1);
            vid->SetRing(0);
            vid->SetDomain(0);
            vid->SetVDisk(0);
            auto *loc = vdisk->MutableVDiskLocation();
            loc->SetNodeID(nodeId);
            loc->SetPDiskID(pdiskId);
            loc->SetVDiskSlotID(0);
            loc->SetPDiskGuid(nodeId * 1000 + pdiskId);

            auto *group = ss->AddGroups();
            group->SetGroupID(0);
            group->SetGroupGeneration(1);
            group->SetErasureSpecies(TBlobStorageGroupType::ErasureNone);
            auto *ring = group->AddRings();
            auto *fd = ring->AddFailDomains();
            auto *gloc = fd->AddVDiskLocations();
            gloc->SetNodeID(nodeId);
            gloc->SetPDiskID(pdiskId);
            gloc->SetVDiskSlotID(0);
            gloc->SetPDiskGuid(nodeId * 1000 + pdiskId);
        }

        NBsController::TPDiskId GetGroupVDiskPDisk() const {
            const auto& group = Config.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
            const auto& location = group.GetRings(0).GetFailDomains(0).GetVDiskLocations(0);
            return {location.GetNodeID(), location.GetPDiskID()};
        }

        void AddMultiVDiskGroupOn(const std::vector<ui32>& nodeIds, i32 erasureSpecies) {
            AddMultiVDiskGroup(Config, nodeIds, erasureSpecies);
        }

        std::vector<ui32> GetGroupDomainNodes() const {
            std::vector<ui32> result;
            const auto& group = Config.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
            for (const auto& fd : group.GetRings(0).GetFailDomains()) {
                result.push_back(fd.GetVDiskLocations(0).GetNodeID());
            }
            return result;
        }

        void SetPDiskType(ui32 nodeId, NKikimrBlobStorage::EPDiskType type) {
            for (auto& pdisk : *BaseConfig.MutablePDisk()) {
                if (pdisk.GetNodeId() == nodeId) {
                    pdisk.SetType(type);
                    return;
                }
            }
            UNIT_FAIL("PDisk not found");
        }

        void SetNodeConnected(ui32 nodeId, bool connected) {
            for (auto& node : *BaseConfig.MutableNode()) {
                if (node.GetNodeId() == nodeId) {
                    node.SetConnected(connected);
                    return;
                }
            }
            UNIT_FAIL("Node not found");
        }

        void SetPDiskState(ui32 nodeId, NKikimrBlobStorage::TPDiskState::E state) {
            for (auto& pdisk : *BaseConfig.MutablePDisk()) {
                if (pdisk.GetNodeId() == nodeId) {
                    pdisk.MutablePDiskMetrics()->SetState(state);
                    return;
                }
            }
            UNIT_FAIL("PDisk not found");
        }

        void SetPDiskDriveStatus(ui32 nodeId, NKikimrBlobStorage::EDriveStatus status) {
            for (auto& pdisk : *BaseConfig.MutablePDisk()) {
                if (pdisk.GetNodeId() == nodeId) {
                    pdisk.SetDriveStatus(status);
                    return;
                }
            }
            UNIT_FAIL("PDisk not found");
        }

        void SetPDiskMaintenanceStatus(ui32 nodeId, NKikimrBlobStorage::TMaintenanceStatus::E status) {
            for (auto& pdisk : *BaseConfig.MutablePDisk()) {
                if (pdisk.GetNodeId() == nodeId) {
                    pdisk.SetMaintenanceStatus(status);
                    return;
                }
            }
            UNIT_FAIL("PDisk not found");
        }

        ui32 GetGroupGeneration() const {
            return Config.GetBlobStorageConfig().GetServiceSet().GetGroups(0).GetGroupGeneration();
        }
    };

    NKikimrBlobStorage::TGroupGeometry Geometry(ui32 numFailDomains) {
        NKikimrBlobStorage::TGroupGeometry g;
        g.SetNumFailRealms(1);
        g.SetNumFailDomainsPerFailRealm(numFailDomains);
        g.SetNumVDisksPerFailDomain(1);
        return g;
    }

    struct TReallocateOptions {
        i32 ErasureSpecies = TBlobStorageGroupType::ErasureNone;
        ui32 NumFailDomains = 1;
        bool AllowUnusableDisks = false;
        bool SettleOnlyOnOperationalDisks = false;
        NBsController::TPDiskId TargetPDiskId;
        TVDiskIdShort VDiskId = TVDiskIdShort(0, 0, 0);
        NKikimr::NStorage::TDistributedConfigKeeper::TStaticGroupReassignments *Reassignments = nullptr;
        bool PreferLessOccupiedRack = false;
        bool WithAttentionToReplication = false;
        bool UseSelfHealLocalPolicy = false;
        bool TryToRelocateBrokenDisksLocallyFirst = false;
    };

    void Reallocate(TSetup& s, const NProtoBuf::RepeatedField<ui32>& allowedNodeIds, bool applyNodeAllowList,
            TReallocateOptions options = {}) {
        auto *selfManagementConfig = s.Config.MutableSelfManagementConfig();
        selfManagementConfig->MutableGeometry()->CopyFrom(Geometry(options.NumFailDomains));
        selfManagementConfig->SetPDiskType(NKikimrBlobStorage::EPDiskType::ROT);
        selfManagementConfig->MutableStaticGroupSelfHealAllowedNodes()->CopyFrom(allowedNodeIds);

        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
        replacedDisks.emplace(options.VDiskId, options.TargetPDiskId);
        NBsController::TGroupMapper::TForbiddenPDisks forbid;
        keeper.AllocateStaticGroup({
            .Config = &s.Config,
            .GroupId = TGroupId::FromValue(0),
            .GroupGeneration = 2,
            .GroupType = TBlobStorageGroupType(static_cast<TBlobStorageGroupType::EErasureSpecies>(options.ErasureSpecies)),
            .ReplacedDisks = std::move(replacedDisks),
            .ForbiddenPDisks = std::move(forbid),
            .BaseConfig = &s.BaseConfig,
            .IgnoreVSlotQuotaCheck = true,
            .AllowUnusableDisks = options.AllowUnusableDisks,
            .SettleOnlyOnOperationalDisks = options.SettleOnlyOnOperationalDisks,
            .PreferLessOccupiedRack = options.PreferLessOccupiedRack,
            .WithAttentionToReplication = options.WithAttentionToReplication,
            .UseSelfHealLocalPolicy = options.UseSelfHealLocalPolicy,
            .TryToRelocateBrokenDisksLocallyFirst = options.TryToRelocateBrokenDisksLocallyFirst,
            .ApplySelfHealNodeAllowList = applyNodeAllowList,
            .Reassignments = options.Reassignments,
        });
    }

    NProtoBuf::RepeatedField<ui32> NodeIds(const std::vector<ui32>& nodeIds) {
        NProtoBuf::RepeatedField<ui32> result;
        for (ui32 nodeId : nodeIds) {
            result.Add(nodeId);
        }
        return result;
    }

    TSetup MakeSetup() {
        TSetup s;
        for (ui32 i = 1; i <= 5; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddCandidatePDisk(i, 1);
        }
        s.AddGroupOn(1, 1);
        return s;
    }

    TSetup MakeBlock42Setup() {
        TSetup s;
        for (ui32 i = 1; i <= 10; ++i) {
            s.AddNode(i, "host-" + std::to_string(i), 19001, "dc1", "rack-" + std::to_string(i));
            s.AddCandidatePDisk(i, 1);
        }
        s.AddMultiVDiskGroupOn({1, 2, 3, 4, 5, 6, 7, 8}, TBlobStorageGroupType::Erasure4Plus2Block);
        return s;
    }

    Y_UNIT_TEST(RespectsNodeAllowList) {
        TSetup s = MakeSetup();
        Reallocate(s, NodeIds({3}), /*applyNodeAllowList=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk().NodeId, 3u);
    }

    Y_UNIT_TEST(RespectsNodeAllowListAnotherNode) {
        TSetup s = MakeSetup();
        Reallocate(s, NodeIds({4}), /*applyNodeAllowList=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk().NodeId, 4u);
    }

    Y_UNIT_TEST(FailsWhenNoAllowedNodeAvailable) {
        TSetup s = MakeSetup();
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({999}), /*applyNodeAllowList=*/ true),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(NoRestrictionWhenAllowListNotApplied) {
        TSetup s = MakeSetup();
        UNIT_ASSERT_NO_EXCEPTION(
            Reallocate(s, NodeIds({999}), /*applyNodeAllowList=*/ false));
    }

    Y_UNIT_TEST(EmptyAllowListMeansNoRestriction) {
        TSetup s = MakeSetup();
        UNIT_ASSERT_NO_EXCEPTION(Reallocate(s, NodeIds({}), /*applyNodeAllowList=*/ true));
    }

    Y_UNIT_TEST(PrefersOperationalPDisk) {
        TSetup s = MakeSetup();
        s.SetNodeConnected(2, false);
        s.SetNodeConnected(4, false);
        s.SetNodeConnected(5, false);
        Reallocate(s, NodeIds({}), false);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk().NodeId, 3u);
    }

    Y_UNIT_TEST(FallsBackToNonOperationalPDiskByDefault) {
        TSetup s = MakeSetup();
        for (ui32 nodeId = 2; nodeId <= 5; ++nodeId) {
            s.SetNodeConnected(nodeId, false);
        }
        UNIT_ASSERT_NO_EXCEPTION(Reallocate(s, NodeIds({}), false));
    }

    Y_UNIT_TEST(FailsWithoutOperationalPDiskWhenRequested) {
        TSetup s = MakeSetup();
        for (ui32 nodeId = 2; nodeId <= 5; ++nodeId) {
            s.SetNodeConnected(nodeId, false);
        }
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {.SettleOnlyOnOperationalDisks = true}),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(RejectsNonNormalPDiskWhenOperationalRequired) {
        TSetup s = MakeSetup();
        s.SetPDiskState(2, NKikimrBlobStorage::TPDiskState::OpenFileError);
        for (ui32 nodeId = 3; nodeId <= 5; ++nodeId) {
            s.SetNodeConnected(nodeId, false);
        }
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {.SettleOnlyOnOperationalDisks = true}),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(RejectsUnknownVDiskWithoutChangingGeneration) {
        TSetup s = MakeSetup();
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {.VDiskId = TVDiskIdShort(0, 1, 0)}),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupGeneration(), 1u);
    }

    Y_UNIT_TEST(RejectsUnusableExplicitTarget) {
        TSetup s = MakeSetup();
        s.SetPDiskDriveStatus(2, NKikimrBlobStorage::EDriveStatus::INACTIVE);
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {.TargetPDiskId = NBsController::TPDiskId(2, 1)}),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(RejectsNonOperationalExplicitTargetWhenRequested) {
        TSetup s = MakeSetup();
        s.SetNodeConnected(2, false);
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {
                .SettleOnlyOnOperationalDisks = true,
                .TargetPDiskId = NBsController::TPDiskId(2, 1),
            }),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(UsesValidExplicitTarget) {
        TSetup s = MakeSetup();
        NKikimr::NStorage::TDistributedConfigKeeper::TStaticGroupReassignments reassignments;
        UNIT_ASSERT_NO_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {
                .TargetPDiskId = NBsController::TPDiskId(3, 1),
                .Reassignments = &reassignments,
            }));
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk().NodeId, 3u);

        UNIT_ASSERT_VALUES_EQUAL(reassignments.size(), 1u);
        const auto it = reassignments.find(TVDiskIdShort(0, 0, 0));
        UNIT_ASSERT(it != reassignments.end());
        UNIT_ASSERT(it->second.SourceSlotId);
        UNIT_ASSERT(it->second.TargetSlotId);
        UNIT_ASSERT_VALUES_EQUAL(it->second.SourceSlotId->GetNodeId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(it->second.SourceSlotId->GetPDiskId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(it->second.SourceSlotId->GetVSlotId(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(it->second.TargetSlotId->GetNodeId(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(it->second.TargetSlotId->GetPDiskId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(it->second.TargetSlotId->GetVSlotId(), 1u);
    }

    Y_UNIT_TEST(Block42KeepsExistingVDisksOnNonAllowedNodes) {
        TSetup s = MakeBlock42Setup();
        Reallocate(s, NodeIds({10}), /*applyNodeAllowList=*/ true, {
            .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
            .NumFailDomains = 8,
        });

        const std::vector<ui32> domainNodes = s.GetGroupDomainNodes();
        UNIT_ASSERT_VALUES_EQUAL(domainNodes.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(domainNodes[0], 10u);
        for (ui32 domainIdx = 1; domainIdx < 8; ++domainIdx) {
            UNIT_ASSERT_VALUES_EQUAL(domainNodes[domainIdx], domainIdx + 1);
        }
    }

    Y_UNIT_TEST(Block42FailsWhenAllowedNodeConflictsWithExistingDomain) {
        TSetup s = MakeBlock42Setup();
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({2}), /*applyNodeAllowList=*/ true, {
                .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
                .NumFailDomains = 8,
            }),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(Block42RejectsUnusablePreservedPDiskByDefault) {
        TSetup s = MakeBlock42Setup();
        s.SetPDiskType(2, NKikimrBlobStorage::EPDiskType::SSD);
        UNIT_ASSERT_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {
                .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
                .NumFailDomains = 8,
            }),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(Block42AllowsUnusablePreservedPDiskWhenRequested) {
        TSetup s = MakeBlock42Setup();
        s.SetPDiskType(2, NKikimrBlobStorage::EPDiskType::SSD);
        UNIT_ASSERT_NO_EXCEPTION(
            Reallocate(s, NodeIds({}), false, {
                .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
                .NumFailDomains = 8,
                .AllowUnusableDisks = true,
            }));
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes()[1], 2u);
    }

    Y_UNIT_TEST(RelocatesBrokenDiskLocallyFirst) {
        TSetup s = MakeSetup();
        s.AddCandidatePDisk(1, 2);

        Reallocate(s, NodeIds({}), false, {
            .TryToRelocateBrokenDisksLocallyFirst = true,
        });

        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk(), NBsController::TPDiskId(1, 2));
    }

    Y_UNIT_TEST(SelfHealLocalPolicyDoesNotFallbackToAnotherNode) {
        TSetup s = MakeSetup();

        UNIT_ASSERT_EXCEPTION(Reallocate(s, NodeIds({}), false, {.UseSelfHealLocalPolicy = true}),
                              NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(RejectsMaintenancePDiskWhenOperationalRequired) {
        TSetup s = MakeSetup();
        s.SetPDiskMaintenanceStatus(2, NKikimrBlobStorage::TMaintenanceStatus::NO_NEW_VDISKS);
        for (ui32 nodeId = 3; nodeId <= 5; ++nodeId) {
            s.SetNodeConnected(nodeId, false);
        }

        UNIT_ASSERT_EXCEPTION(Reallocate(s, NodeIds({}), false, {.SettleOnlyOnOperationalDisks = true}),
                              NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }

    Y_UNIT_TEST(DestroyedServiceSetDuplicateDoesNotAffectLocality) {
        TSetup s = MakeBlock42Setup();

        auto addBaseConfigVSlot = [&](ui32 groupId, ui32 nodeId, ui32 vslotId) {
            auto *vslot = s.BaseConfig.AddVSlot();
            vslot->MutableVSlotId()->SetNodeId(nodeId);
            vslot->MutableVSlotId()->SetPDiskId(1);
            vslot->MutableVSlotId()->SetVSlotId(vslotId);
            vslot->SetGroupId(groupId);
            vslot->SetGroupGeneration(1);
            vslot->SetFailRealmIdx(0);
            vslot->SetFailDomainIdx(0);
            vslot->SetVDiskIdx(0);
            vslot->SetStatus("READY");
        };

        for (ui32 groupId : {42u, 43u}) {
            auto *group = s.BaseConfig.AddGroup();
            group->SetGroupId(groupId);
            group->SetGroupGeneration(1);
            group->SetGroupSizeInUnits(1);
        }

        addBaseConfigVSlot(42, 2, 7); // group shared with a preserved PDisk
        addBaseConfigVSlot(42, 10, 7); // stale locality on one replacement candidate
        addBaseConfigVSlot(43, 9, 7); // keep candidate slot counts equal

        auto *destroyed = s.Config.MutableBlobStorageConfig()->MutableServiceSet()->AddVDisks();
        VDiskIDFromVDiskID(TVDiskID(TGroupId::FromValue(42), 1, 0, 0, 0), destroyed->MutableVDiskID());
        destroyed->MutableVDiskLocation()->SetNodeID(10);
        destroyed->MutableVDiskLocation()->SetPDiskID(1);
        destroyed->MutableVDiskLocation()->SetVDiskSlotID(7);
        destroyed->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);

        Reallocate(s, NodeIds({}), false, {
            .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
            .NumFailDomains = 8,
        });

        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes()[0], 9u);
    }

    Y_UNIT_TEST(UsesGroupsAndVSlotsFromBaseConfigSnapshot) {
        TSetup s = MakeSetup();

        auto addBaseConfigVSlot = [&](ui32 groupId, ui32 groupSizeInUnits, ui32 nodeId, ui32 vslotId,
                                      TStringBuf status) {
            auto *vslot = s.BaseConfig.AddVSlot();
            vslot->MutableVSlotId()->SetNodeId(nodeId);
            vslot->MutableVSlotId()->SetPDiskId(1);
            vslot->MutableVSlotId()->SetVSlotId(vslotId);
            vslot->SetGroupId(groupId);
            vslot->SetGroupGeneration(1);
            vslot->SetFailRealmIdx(0);
            vslot->SetFailDomainIdx(0);
            vslot->SetVDiskIdx(0);
            vslot->SetStatus(TString(status));
            vslot->MutableVDiskMetrics()->SetAllocatedSize(100);

            auto *group = s.BaseConfig.AddGroup();
            group->SetGroupId(groupId);
            group->SetGroupGeneration(1);
            group->SetGroupSizeInUnits(groupSizeInUnits);
            group->AddVSlotId()->CopyFrom(vslot->GetVSlotId());
        };

        // This entry duplicates the static VSlot from ServiceSet, as a real BSC BaseConfig does.
        addBaseConfigVSlot(0, 1, 1, 0, "READY");
        // A three-unit dynamic group fully occupies PDisk (2, 1). Without importing GroupSizeInUnits
        // the mapper would count it as one slot and incorrectly accept it as an explicit target.
        addBaseConfigVSlot(42, 3, 2, 7, "REPLICATING");
        for (auto& pdisk : *s.BaseConfig.MutablePDisk()) {
            if (pdisk.GetNodeId() == 2) {
                pdisk.SetExpectedSlotCount(3);
                pdisk.MutablePDiskConfig()->SetSlotSizeInUnits(1);
            }
        }

        try {
            Reallocate(s, NodeIds({}), false, {
                .TargetPDiskId = NBsController::TPDiskId(2, 1),
                .PreferLessOccupiedRack = true,
                .WithAttentionToReplication = true,
            });
            UNIT_FAIL("Expected the weighted VSlot to exhaust the target PDisk");
        } catch (const NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError& ex) {
            const TString error = ex.what();
            UNIT_ASSERT_C(error.Contains("2:1-s[3/3]"), error);
        }
    }
}

}
}
