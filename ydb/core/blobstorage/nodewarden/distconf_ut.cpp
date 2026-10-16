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

#include <array>

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
                            i32 erasureSpecies, ui32 groupId = 0) {
        auto *serviceSet = config.MutableBlobStorageConfig()->MutableServiceSet();
        auto *group = serviceSet->AddGroups();
        group->SetGroupID(groupId);
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
            vdiskId->SetGroupID(groupId);
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

    TIntrusivePtr<TNodeWardenConfig> MakeNodeWardenConfig(const NKikimrBlobStorage::TStorageConfig& storageConfig,
            const NKikimrConfig::TBridgeConfig *bridgeConfig = nullptr) {
        auto config = MakeIntrusive<TNodeWardenConfig>(TIntrusivePtr<IPDiskServiceFactory>{});
        if (bridgeConfig) {
            config->BridgeConfig = *bridgeConfig;
        }
        for (const auto& node : storageConfig.GetAllNodes()) {
            auto *nsNode = config->NameserviceConfig.AddNode();
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

    struct TNodeWardenTestState {
        THashSet<ui32> MobileNodes;
        THashMap<std::pair<ui32, TString>, NKikimrBlobStorage::TPDiskMetadataRecord> Metadata;
        ui32 RequiredRootNodeId = 0;
        bool SawCompleteMobileBinding = false;
        bool SawScatter = false;

        TNodeWardenTestState() = default;

        explicit TNodeWardenTestState(std::initializer_list<ui32> mobileNodes)
            : MobileNodes(mobileNodes.begin(), mobileNodes.end())
        {}

        bool ShouldRejectBinding(ui32 senderNodeId, ui32 recipientNodeId,
                                 const NKikimrBlobStorage::TEvNodeConfigPush& record) {
            if (RequiredRootNodeId) {
                return recipientNodeId != RequiredRootNodeId;
            }
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
        TNodeWardenTestState& State;

        void RejectBinding(const IEventHandle& request) {
            auto response = NStorage::TEvNodeConfigReversePush::MakeRejected();
            auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(request.Sender.NodeId()), SelfId(),
                                                         response.release(), 0, request.Cookie);
            handle->Rewrite(TEvInterconnect::EvForward, request.InterconnectSession);
            TActivationContext::Send(handle.release());
        }

    public:
        TNodeWardenProxyActor(TActorId keeperId, TNodeWardenTestState& state)
            : TActor(&TThis::StateFunc)
            , KeeperId(keeperId)
            , State(state)
        {}

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (!ev->InterconnectSession) {
                if (type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                    const auto *msg = ev->Get<NStorage::TEvNodeWardenReadMetadata>();
                    const auto it = State.Metadata.find(std::make_pair(SelfId().NodeId(), msg->Path));
                    const bool found = it != State.Metadata.end();
                    Send(ev->Sender, new NStorage::TEvNodeWardenReadMetadataResult(
                        std::nullopt, found ? NPDisk::EPDiskMetadataOutcome::OK : NPDisk::EPDiskMetadataOutcome::NO_METADATA,
                        found ? it->second : NKikimrBlobStorage::TPDiskMetadataRecord()), 0, ev->Cookie);
                } else if (type == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                    const auto *msg = ev->Get<NStorage::TEvNodeWardenWriteMetadata>();
                    State.Metadata[std::make_pair(SelfId().NodeId(), msg->Path)] = msg->Record;
                    Send(ev->Sender, new NStorage::TEvNodeWardenWriteMetadataResult(
                        std::nullopt, NPDisk::EPDiskMetadataOutcome::OK), 0, ev->Cookie);
                }
                return;
            }

            if (type == NStorage::TEvNodeConfigPush::EventType) {
                const auto *msg = ev->Get<NStorage::TEvNodeConfigPush>();
                if (msg->Record.GetInitial() && State.ShouldRejectBinding(ev->Sender.NodeId(), SelfId().NodeId(), msg->Record)) {
                    RejectBinding(*ev);
                    return;
                }
            } else if (type == NStorage::TEvNodeConfigScatter::EventType) {
                State.SawScatter = true;
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
                            TNodeWardenTestState& state, const TString& startupConfigYaml = {},
                            const NKikimrConfig::TBridgeConfig *bridgeConfig = nullptr) {
        auto nodeWardenConfig = MakeNodeWardenConfig(config, bridgeConfig);
        nodeWardenConfig->StartupConfigYaml = startupConfigYaml;
        auto storageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(config);
        const TActorId keeperId = runtime.Register(
            new NStorage::TDistributedConfigKeeper(std::move(nodeWardenConfig), storageConfig, true), nodeId);

        const TActorId nodeWardenProxyId = runtime.Register(new TNodeWardenProxyActor(keeperId, state), nodeId);
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

    template<typename TCondition>
    bool SimUntil(TTestActorSystem& runtime, TCondition&& condition, TDuration timeout = TDuration::Seconds(30),
                  ui32 nodeId = 1) {
        const TInstant deadline = runtime.GetClock() + timeout;
        runtime.Schedule(deadline, new IEventHandle(TEvents::TSystem::Wakeup, 0, {}, {}, nullptr, 0), nullptr, nodeId);
        runtime.Sim([&] { return !condition() && runtime.GetClock() < deadline; });
        return condition();
    }

    void CheckStaysConverged(TTestActorSystem& runtime, const std::vector<TActorId>& keeperIds) {
        UNIT_ASSERT_C(!SimUntil(runtime, [&] { return !FindConvergedRoot(runtime, keeperIds); },
                                TDuration::Seconds(30), keeperIds.front().NodeId()),
                      "binding tree lost convergence without new failures");
    }

    auto InvokeKeeper(TTestActorSystem& runtime, TActorId keeperId,
                      std::unique_ptr<NStorage::TEvNodeConfigInvokeOnRoot> request) {
        const TActorId edge = runtime.AllocateEdgeActor(keeperId.NodeId());
        runtime.Send(new IEventHandle(keeperId, edge, request.release()), keeperId.NodeId());
        auto response = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
        UNIT_ASSERT_C(response->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      response->Get()->Record.DebugString());
        return response;
    }

    void BootstrapCluster(TTestActorSystem& runtime, TActorId keeperId) {
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableBootstrapCluster()->SetSelfAssemblyUUID("partial-bootstrap-test");
        InvokeKeeper(runtime, keeperId, std::move(request));
    }

    NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult QueryKeeper(TTestActorSystem& runtime, TActorId keeperId,
                                                                    bool workingRootOnly = false) {
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        if (workingRootOnly) {
            request->Record.MutableQueryWorkingRoot();
        } else {
            request->Record.MutableQueryConfig();
        }
        return InvokeKeeper(runtime, keeperId, std::move(request))->Get()->Record;
    }

    ui32 RunUntilConverged(const NKikimrBlobStorage::TStorageConfig& config, TNodeWardenTestState& state,
                           TDuration timeout = TDuration::Seconds(30), bool waitForScatter = false) {
        TTestActorSystem runtime(config.AllNodesSize());
        runtime.Start();
        Y_DEFER {
            runtime.Stop();
        };

        std::vector<TActorId> keeperIds;
        for (ui32 nodeId = 1; nodeId <= static_cast<ui32>(config.AllNodesSize()); ++nodeId) {
            keeperIds.push_back(RegisterKeeper(runtime, config, nodeId, state));
        }

        std::optional<ui32> rootNodeId;
        const bool converged = SimUntil(runtime, [&] {
            rootNodeId = FindConvergedRoot(runtime, keeperIds);
            return rootNodeId && (!waitForScatter || state.SawScatter);
        }, timeout);
        UNIT_ASSERT_C(converged, "binding tree did not converge before the deadline");
        CheckStaysConverged(runtime, keeperIds);
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
        TNodeWardenTestState state;
        const ui32 rootNodeId = RunUntilConverged(config, state, TDuration::Seconds(30), true);
        UNIT_ASSERT(state.SawScatter);
        UNIT_ASSERT(1 <= rootNodeId && rootNodeId <= 3);
    }

    void CheckInitialConfigBindingCycle(TDuration unbindDelay) {
        const auto config = MakeInitialStorageConfig(3);
        TNodeWardenTestState state;
        TTestActorSystem runtime(3);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        std::vector<std::unique_ptr<IEventHandle>> pending;
        std::vector<std::unique_ptr<IEventHandle>> delayed;
        std::array<bool, 4> initialDelivered{};
        IEventHandle *released = nullptr;
        bool hold = true;
        bool delayUnbind = false;

        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId > keepers.size() || ev->GetRecipientRewrite() != keepers[nodeId - 1]) {
                return true;
            }
            if (delayUnbind && nodeId == 1 && ev->Sender.NodeId() == 2 && ev->InterconnectSession) {
                delayed.push_back(std::move(ev));
                return false;
            }
            if (!hold) {
                return true;
            }
            if (ev.get() == released) {
                released = nullptr;
                return true;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeConfigReversePush::EventType
                || type == NStorage::TEvNodeConfigUnbind::EventType
                || type == NStorage::TEvNodeConfigScatter::EventType
                || (type == NStorage::TEvNodeConfigPush::EventType
                    && (ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetInitial()
                        || !initialDelivered[ev->Sender.NodeId()]))) {
                pending.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
            keepers.push_back(RegisterKeeper(runtime, config, nodeId, state));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return pending.size() == 3; }));
        pending.clear(); // Discard undelivered attempts.

        // Form 1 -> 3 -> 2 -> 1.
        for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
            UNIT_ASSERT(runtime.WrapInActorContext(keepers[nodeId - 1], [&](IActor *actor) {
                auto *keeper = dynamic_cast<NStorage::TDistributedConfigKeeper*>(actor);
                UNIT_ASSERT(keeper);
                keeper->AbortBinding("prepare binding cycle", false, false);
                keeper->StartBinding(nodeId == 1 ? 3 : nodeId - 1);
            }));
        }

        const auto deliver = [&](ui32 type, ui32 sender, ui32 recipient, ui32 root = 0) {
            const auto matches = [&](const auto& ev) {
                return ev->GetTypeRewrite() == type && ev->Sender.NodeId() == sender
                       && ev->GetRecipientRewrite() == keepers[recipient - 1]
                       && (!root || ev->template Get<NStorage::TEvNodeConfigReversePush>()->Record.GetRootNodeId() == root);
            };
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return std::ranges::any_of(pending, matches); },
                                   TDuration::Seconds(1)),
                          "missing cycle event " << type << " " << sender << "->" << recipient << " root=" << root);
            const auto it = std::ranges::find_if(pending, matches);
            if (root) {
                UNIT_ASSERT(!(*it)->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetRejected());
            }
            released = it->release();
            pending.erase(it);
            runtime.Send(released, recipient);
            UNIT_ASSERT(SimUntil(runtime, [&] { return !released; }, TDuration::Seconds(1)));
        };
        for (ui32 nodeId : {1, 3, 2}) {
            deliver(NStorage::TEvNodeConfigPush::EventType, nodeId, nodeId == 1 ? 3 : nodeId - 1);
            initialDelivered[nodeId] = true;
            std::erase_if(pending, [&](auto& ev) {
                if (ev->Sender.NodeId() != nodeId || ev->GetTypeRewrite() != NStorage::TEvNodeConfigPush::EventType) {
                    return false;
                }
                const ui32 recipient = ev->GetRecipientRewrite().NodeId();
                runtime.Send(ev.release(), recipient);
                return true;
            });
        }
        // Replay SPIN's root announcements, preserving each sender's order.
        for (const auto& [sender, recipient, root] : std::initializer_list<std::tuple<ui32, ui32, ui32>>{
                 {2, 3, 2}, {1, 2, 1}, {2, 3, 1}, {3, 1, 3}, {1, 2, 3}, {3, 1, 2},
                 {2, 3, 3}, {1, 2, 2}, {3, 1, 1}}) {
            deliver(NStorage::TEvNodeConfigReversePush::EventType, sender, recipient, root);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return std::ranges::count_if(pending, [](const auto& ev) {
                return ev->GetTypeRewrite() == NStorage::TEvNodeConfigUnbind::EventType;
            }) == 3;
        }, TDuration::Seconds(1)));
        for (TActorId id : keepers) {
            UNIT_ASSERT(runtime.WrapInActorContext(id, [&](IActor *actor) {
                UNIT_ASSERT_VALUES_EQUAL(static_cast<NStorage::TDistributedConfigKeeper*>(actor)->GetRootNodeId(),
                                         id.NodeId());
            }));
        }

        hold = false;
        delayUnbind = true;
        for (auto& ev : pending) {
            const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
            runtime.Send(ev.release(), nodeId);
        }
        // Keep Initial behind Unbind by delaying the entire 2 -> 1 stream.
        SimUntil(runtime, [] { return false; }, unbindDelay);
        if (unbindDelay) {
            UNIT_ASSERT(!delayed.empty());
            UNIT_ASSERT_VALUES_EQUAL(delayed.front()->GetTypeRewrite(), NStorage::TEvNodeConfigUnbind::EventType);
        }
        delayUnbind = false;
        for (auto& ev : delayed) {
            runtime.Send(ev.release(), 1);
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers).has_value(); }),
                      "initial-config binding ring did not recover; unbindDelay=" << unbindDelay);
        CheckStaysConverged(runtime, keepers);
        const auto root = *FindConvergedRoot(runtime, keepers);
        const auto result = QueryKeeper(runtime, keepers[root - 1]);
        UNIT_ASSERT(result.HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(result.GetQueryConfig().GetConfig().GetFingerprint(), config.GetFingerprint());
    }

    Y_UNIT_TEST(InitialConfigBindingCycleRecovers) {
        CheckInitialConfigBindingCycle(TDuration::Zero());
    }

    Y_UNIT_TEST(InitialConfigBindingCycleRecoversWithLateUnbind) {
        CheckInitialConfigBindingCycle(TDuration::Seconds(5));
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

    void CheckKnownBoundNodeAddedToBindQueue(bool otherPile) {
        const auto initial = MakeStorageConfig(2, 1, {1});
        auto expanded = MakeStorageConfig(3, 2, {1});
        if (otherPile) {
            expanded.MutableAllNodes(2)->MutableLocation()->SetBridgePileName("other");
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&expanded);
        }

        TNodeWardenTestState state;
        TTestActorSystem runtime(2);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        const TActorId keeperId = RegisterKeeper(runtime, initial, 1, state);
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, {keeperId}) == 1; }));

        bool boundNodePicked = false;
        std::optional<ui32> pickedAfterUnbind;
        UNIT_ASSERT(runtime.WrapInActorContext(keeperId, [&](IActor *actor) {
            auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
            const NStorage::TNodeIdentifier addedNode(expanded.GetAllNodes(2));

            keeper->UpdateBound(2, addedNode, expanded, nullptr);
            std::vector<std::tuple<NStorage::TNodeIdentifier, TNodeLocation>> nodes;
            for (const auto& node : expanded.GetAllNodes()) {
                nodes.emplace_back(node, node.GetLocation());
            }

            const TMonotonic now = TActivationContext::Monotonic();
            for (ui32 update = 0; update != 2; ++update) {
                keeper->ApplyNewNodeList(nodes);
                auto *queue = keeper->GetBindQueue(addedNode.NodeId());
                UNIT_ASSERT(queue);
                TMonotonic closest;
                while (const auto nodeId = queue->Pick(now, &closest)) {
                    if (*nodeId == addedNode.NodeId()) {
                        boundNodePicked = true;
                        return;
                    }
                }
            }

            keeper->DeleteBound(2, addedNode, nullptr);
            TMonotonic closest;
            pickedAfterUnbind = keeper->GetBindQueue(addedNode.NodeId())->Pick(now, &closest);
        }));
        UNIT_ASSERT_C(!boundNodePicked, "already bound node became a binding candidate");
        UNIT_ASSERT(pickedAfterUnbind);
        UNIT_ASSERT_VALUES_EQUAL(*pickedAfterUnbind, 3u);
    }

    Y_UNIT_TEST(KnownBoundNodeAddedToSamePileQueue) {
        CheckKnownBoundNodeAddedToBindQueue(false);
    }

    Y_UNIT_TEST(KnownBoundNodeAddedToOtherPileQueue) {
        CheckKnownBoundNodeAddedToBindQueue(true);
    }

    Y_UNIT_TEST(ChildRemovalWhileAddingIndirectNode) {
        auto initial = MakeStorageConfig(4, 1, {3});
        initial.MutableAllNodes()->DeleteSubrange(1, 1);
        initial.MutableBlobStorageConfig()->MutableDefineBox()->MutableHost()->DeleteSubrange(1, 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&initial);
        const auto intermediate = MakeStorageConfig(4, 2, {3});
        const auto updated = MakeStorageConfig(3, 3, {3});

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 3;
        TTestActorSystem runtime(4);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        const TActorId keeperId = RegisterKeeper(runtime, initial, 3, state);
        const TActorId childId = runtime.Register(new TIgnoreActor, 4);
        runtime.RegisterService(MakeBlobStorageNodeWardenID(4), childId);
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, {keeperId}) == 3; }));

        bool pushDelivered = false;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId == keeperId.NodeId() && ev->InterconnectSession
                && ev->GetRecipientRewrite() == keeperId && ev->Sender == childId
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigPush::EventType) {
                pushDelivered = true;
            }
            return true;
        };
        auto push = std::make_unique<NStorage::TEvNodeConfigPush>();
        push->Record.SetInitial(true);
        for (ui32 nodeId : {4u, 2u}) {
            auto *bound = push->Record.AddBoundNodes();
            bound->MutableNodeId()->CopyFrom(intermediate.GetAllNodes(nodeId - 1));
            bound->MutableMeta()->CopyFrom(NStorage::TStorageConfigMeta(intermediate));
        }
        runtime.Send(new IEventHandle(keeperId, childId, push.release(), 0, 1), 4);
        UNIT_ASSERT(SimUntil(runtime, [&] { return pushDelivered; }));

        bool applied = false;
        bool childRemoved = false;
        THashSet<ui32> candidates;
        UNIT_ASSERT(runtime.WrapInActorContext(keeperId, [&](IActor *actor) {
            auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
            applied = keeper->ApplyStorageConfig(updated);
            childRemoved = !keeper->GetBindQueue(4);
            auto *queue = keeper->GetBindQueue(2);
            UNIT_ASSERT(queue);
            const TMonotonic now = TActivationContext::Monotonic();
            TMonotonic closest;
            while (const auto nodeId = queue->Pick(now, &closest)) {
                candidates.insert(*nodeId);
            }
        }));
        UNIT_ASSERT(applied);
        UNIT_ASSERT(childRemoved);
        UNIT_ASSERT(candidates.contains(2));

        const TActorId addedKeeperId = RegisterKeeper(runtime, updated, 2, state);
        const std::vector keepers{keeperId, addedKeeperId};
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 3; }));
        CheckStaysConverged(runtime, keepers);
    }

    Y_UNIT_TEST(DelayedConnectionNotificationDoesNotBreakBridgeGather) {
        auto config = MakeStorageConfig(4);
        config.MutableSelfManagementConfig()->SetEnabled(true);
        for (auto& node : *config.MutableAllNodes()) {
            node.MutableLocation()->SetBridgePileName(node.GetNodeId() == 1 || node.GetNodeId() == 4
                ? "pile0" : "pile1");
            node.MutableLocation()->SetDataCenter("dc0");
        }
        auto *box = config.MutableBlobStorageConfig()->MutableDefineBox();
        box->MutableHost()->DeleteSubrange(1, 1);
        for (ui32 index : {2u, 3u}) {
            auto *drive = config.MutableBlobStorageConfig()->MutableDefineHostConfig(0)->AddDrive();
            drive->SetPath("/dev/disk1_" + std::to_string(index));
            drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
        }
        auto *serviceSet = config.MutableBlobStorageConfig()->MutableServiceSet();
        auto *proxy = serviceSet->AddGroups();
        proxy->SetGroupID(0);
        proxy->SetGroupGeneration(1);
        for (ui32 pileIndex : {0u, 1u}) {
            auto *pile = proxy->MutableBridgeGroupState()->AddPile();
            pile->SetGroupId(pileIndex + 1);
            pile->SetGroupGeneration(1);
            pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
        }
        AddMultiVDiskGroup(config, {1}, TBlobStorageGroupType::ErasureNone, 1);
        AddMultiVDiskGroup(config, {3}, TBlobStorageGroupType::ErasureNone, 2);
        for (ui32 pileIndex : {0u, 1u}) {
            auto *group = serviceSet->MutableGroups(pileIndex + 1);
            group->SetBridgeProxyGroupId(0);
            TBridgePileId::FromPileIndex(pileIndex).CopyToProto(group,
                &NKikimrBlobStorage::TGroupInfo::SetBridgePileId);
        }
        auto *cluster = config.MutableClusterState();
        cluster->SetGeneration(1);
        cluster->AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        cluster->AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        TBridgePileId::FromPileIndex(0).CopyToProto(cluster, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(0).CopyToProto(cluster, &NKikimrBridge::TClusterState::SetPromotedPile);
        config.MutableClusterStateDetails();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        for (ui32 nodeId : {1u, 3u}) {
            state.Metadata[std::make_pair(nodeId, TString("/dev/disk" + std::to_string(nodeId) + "_1"))]
                .MutableCommittedStorageConfig()->CopyFrom(config);
        }
        for (ui32 index : {2u, 3u}) {
            state.Metadata[std::make_pair(1u, TString("/dev/disk1_" + std::to_string(index)))]
                .MutableCommittedStorageConfig()->CopyFrom(config);
        }
        TTestActorSystem runtime(4);
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::array<TActorId, 5> keepers;
        for (ui32 nodeId : {1u, 3u}) {
            keepers[nodeId] = RegisterKeeper(runtime, config, nodeId, state, {}, &bridge);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, {keepers[1], keepers[3]}) == 1; }));
        UNIT_ASSERT(QueryKeeper(runtime, keepers[1]).HasScepter());
        state.RequiredRootNodeId = 0;

        std::vector<std::unique_ptr<IEventHandle>> pending;
        std::vector<std::unique_ptr<IEventHandle>> connected;
        std::unique_ptr<IEventHandle> metadata;
        IEventHandle *released = nullptr;
        bool hold = true;
        bool holdMetadata = false;
        std::unique_ptr<IEventHandle> queryResult;
        TActorId queryEdge;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev.get() == released) {
                released = nullptr;
                return true;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (queryEdge && ev->GetRecipientRewrite() == queryEdge
                && type == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                UNIT_ASSERT(!queryResult);
                queryResult = std::move(ev);
                return false;
            }
            if (nodeId == 4 && holdMetadata && type == NStorage::TEvNodeWardenReadMetadataResult::EventType) {
                UNIT_ASSERT(!metadata);
                metadata = std::move(ev);
                return false;
            }
            if (nodeId == 4 && ev->GetRecipientRewrite() == keepers[4]) {
                if (type == TEvInterconnect::TEvNodeConnected::EventType
                    && ev->Get<TEvInterconnect::TEvNodeConnected>()->NodeId == 2) {
                    connected.push_back(std::move(ev));
                    return false;
                }
            }
            if (hold && nodeId < keepers.size() && ev->InterconnectSession
                && ev->GetRecipientRewrite() == keepers[nodeId]
                && (type == NStorage::TEvNodeConfigPush::EventType
                    || type == NStorage::TEvNodeConfigReversePush::EventType
                    || type == NStorage::TEvNodeConfigUnbind::EventType
                    || type == NStorage::TEvNodeConfigScatter::EventType)) {
                pending.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        for (ui32 nodeId : {2u, 4u}) {
            keepers[nodeId] = RegisterKeeper(runtime, config, nodeId, state, {}, &bridge);
        }
        const auto withKeeper = [&](ui32 nodeId, auto&& action) {
            UNIT_ASSERT(runtime.WrapInActorContext(keepers[nodeId], [&](IActor *actor) {
                action(*static_cast<NStorage::TDistributedConfigKeeper*>(actor));
            }));
        };
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return std::ranges::all_of(std::array{2u, 4u}, [&](ui32 nodeId) {
                return std::ranges::any_of(pending, [&](const auto& ev) {
                    return ev->GetTypeRewrite() == NStorage::TEvNodeConfigPush::EventType
                        && ev->Sender.NodeId() == nodeId && ev->template Get<NStorage::TEvNodeConfigPush>()->Record.GetInitial();
                });
            });
        }));
        runtime.Sim([&] { return runtime.HasImmediateEvents(); });
        pending.clear();
        withKeeper(2, [&](auto& keeper) {
            keeper.AbortBinding("prepare delayed connection test", false, false);
            keeper.StartBinding(4);
        });
        const auto deliver = [&](ui32 type, ui32 sender, ui32 recipient) {
            const auto matches = [&](const auto& ev) {
                return ev->GetTypeRewrite() == type && ev->Sender.NodeId() == sender
                    && ev->GetRecipientRewrite() == keepers[recipient];
            };
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return std::ranges::any_of(pending, matches); }, TDuration::Seconds(1)),
                "missing bridge event " << type << ' ' << sender << "->" << recipient);
            const auto it = std::ranges::find_if(pending, matches);
            released = it->release();
            pending.erase(it);
            runtime.Send(released, recipient);
            UNIT_ASSERT(SimUntil(runtime, [&] { return !released; }, TDuration::Seconds(1)));
        };
        deliver(NStorage::TEvNodeConfigPush::EventType, 2, 4);
        UNIT_ASSERT(SimUntil(runtime, [&] { return !connected.empty(); }, TDuration::Seconds(1)));
        withKeeper(2, [&](auto& keeper) {
            keeper.AbortBinding("prepare root connection", true, false);
            keeper.StartBinding(3);
        });
        withKeeper(4, [&](auto& keeper) {
            keeper.AbortBinding("prepare redirected connection", false, false);
            keeper.StartBinding(2);
        });
        deliver(NStorage::TEvNodeConfigReversePush::EventType, 4, 2);
        deliver(NStorage::TEvNodeConfigPush::EventType, 4, 2);
        for (auto& ev : connected) {
            released = ev.release();
            runtime.Send(released, 4);
            UNIT_ASSERT(SimUntil(runtime, [&] { return !released; }, TDuration::Seconds(1)));
        }
        connected.clear();
        deliver(NStorage::TEvNodeConfigUnbind::EventType, 2, 4);
        deliver(NStorage::TEvNodeConfigReversePush::EventType, 2, 4);
        deliver(NStorage::TEvNodeConfigPush::EventType, 2, 3);
        deliver(NStorage::TEvNodeConfigReversePush::EventType, 3, 2);
        deliver(NStorage::TEvNodeConfigReversePush::EventType, 2, 4);

        holdMetadata = true;
        withKeeper(1, [&](auto& keeper) {
            keeper.Invoke(NStorage::TDistributedConfigKeeper::TCollectConfigsAndPropose{});
        });
        deliver(NStorage::TEvNodeConfigScatter::EventType, 1, 3);
        deliver(NStorage::TEvNodeConfigScatter::EventType, 3, 2);
        deliver(NStorage::TEvNodeConfigScatter::EventType, 2, 4);
        UNIT_ASSERT(SimUntil(runtime, [&] { return bool(metadata); }, TDuration::Seconds(1)));
        queryEdge = runtime.AllocateEdgeActor(1);
        auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        query->Record.MutableQueryConfig();
        runtime.Send(new IEventHandle(keepers[1], queryEdge, query.release()), 1);

        hold = false;
        for (auto& ev : pending) {
            const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
            runtime.Send(ev.release(), nodeId);
        }
        holdMetadata = false;
        pending.clear();
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        runtime.Send(metadata.release(), 4);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return bool(queryResult); }),
            "bridge gather did not finish after the delayed connection notification");
        const auto& result = queryResult->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
        UNIT_ASSERT_C(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.DebugString());
        runtime.DestroyActor(queryEdge);
        const std::vector allKeepers{keepers[1], keepers[2], keepers[3], keepers[4]};
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, allKeepers) == 1; }));
        CheckStaysConverged(runtime, allKeepers);
    }

    Y_UNIT_TEST(ThreeNodeSplitConvergesThroughMailbox) {
        const auto config = MakeStorageConfig(3, 0, {1});
        TNodeWardenTestState state{2, 3};
        const ui32 rootNodeId = RunUntilConverged(config, state);
        UNIT_ASSERT(state.SawCompleteMobileBinding);
        UNIT_ASSERT_VALUES_EQUAL(rootNodeId, 1u);
    }

    void ScheduleSubscriptionFailure(TTestActorSystem& runtime, ui32 nodeId, const IEventHandle& request,
                                     ui32 peerNodeId, TDuration delay) {
        auto response = std::make_unique<IEventHandle>(request.Sender, request.GetRecipientRewrite(),
                                                       new TEvInterconnect::TEvNodeDisconnected(peerNodeId),
                                                       0, request.Cookie);
        runtime.Schedule(delay, response.release(), nullptr, nodeId);
    }

    void CheckRootAfterNodeLoss(bool loseStorageQuorum, bool connectionTimeout) {
        const auto config = MakeStorageConfig(3, 1, {loseStorageQuorum ? 2u : 1u});
        const auto initial = MakeStorageConfig(3, 0);
        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        TTestActorSystem runtime(3);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };

        std::vector<TActorId> keepers;
        for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
            keepers.push_back(RegisterKeeper(runtime, loseStorageQuorum && nodeId != 1 ? initial : config, nodeId, state));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }));
        CheckStaysConverged(runtime, keepers);
        const auto before = QueryKeeper(runtime, keepers.front());
        UNIT_ASSERT(before.HasScepter());

        THashMap<TActorId, ui32> peers;
        runtime.WrapInActorContext(keepers.front(), [&](IActor*) {
            for (ui32 nodeId : {2, 3}) {
                peers.emplace(TActivationContext::InterconnectProxy(nodeId), nodeId);
            }
        });
        THashSet<ui32> attemptedPeers;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Subscribe) {
                if (const auto it = peers.find(ev->GetRecipientRewrite()); it != peers.end()) {
                    attemptedPeers.insert(it->second);
                    if (!connectionTimeout) {
                        ScheduleSubscriptionFailure(runtime, nodeId, *ev, it->second, TDuration::MilliSeconds(10));
                    }
                    return false;
                }
            }
            return true;
        };
        runtime.StopNode(2);
        runtime.StopNode(3);

        if (loseStorageQuorum) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return !FindConvergedRoot(runtime, {keepers.front()}); }));
        } else {
            CheckStaysConverged(runtime, {keepers.front()});
            UNIT_ASSERT(attemptedPeers.empty());
            const auto after = QueryKeeper(runtime, keepers.front());
            UNIT_ASSERT(after.HasScepter());
            UNIT_ASSERT_VALUES_EQUAL(after.GetScepter().GetId(), before.GetScepter().GetId());
            UNIT_ASSERT_VALUES_EQUAL(after.GetQueryConfig().GetConfig().GetFingerprint(), config.GetFingerprint());
        }
    }

    Y_UNIT_TEST(ConfigQuorumLossKeepsEstablishedRoot) {
        CheckRootAfterNodeLoss(false, false);
    }

    Y_UNIT_TEST(EstablishedRootDoesNotProbeUnavailablePeers) {
        CheckRootAfterNodeLoss(false, true);
    }

    Y_UNIT_TEST(StorageQuorumLossStillReleasesRoot) {
        CheckRootAfterNodeLoss(true, false);
    }

    Y_UNIT_TEST(OfflineLowerPeerDoesNotBlockBinding) {
        const auto config = MakeStorageConfig(3, 1, {3});
        TNodeWardenTestState state;
        TTestActorSystem runtime(3);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        runtime.StopNode(1);

        const TActorId follower = RegisterKeeper(runtime, config, 2, state);
        TActorId offlineProxy;
        runtime.WrapInActorContext(follower, [&](IActor*) {
            offlineProxy = TActivationContext::InterconnectProxy(1);
        });
        ui32 attempts = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Subscribe && ev->GetRecipientRewrite() == offlineProxy) {
                ++attempts;
                return false; // neither Connected nor Disconnected: the binding deadline must fire
            }
            return true;
        };
        const TActorId root = RegisterKeeper(runtime, config, 3, state);
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, {follower, root}) == 3; },
                             TDuration::Seconds(30), 2));
        UNIT_ASSERT(attempts);
        CheckStaysConverged(runtime, {follower, root});
    }

    enum class EJoinFault {
        None,
        BusyOperation,
        BusyWorkingRoot,
        ExpiredProbe,
        UnsupportedProbe,
        LostBindingReply,
        DisconnectBeforeBinding,
        ObsoleteDisconnect,
    };

    void CheckJoinWorkingRoot(EJoinFault fault) {
        const auto initial = MakeStorageConfig(3, 0);
        const auto working = MakeStorageConfig(3, 1, {1});
        TNodeWardenTestState state;
        TTestActorSystem runtime(3);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        bool partitioned = true;
        bool allowOldOperation = false;
        TActorId oldOperation;
        std::vector<std::unique_ptr<IEventHandle>> oldOperationEvents;
        bool blockBindingReplies = fault == EJoinFault::LostBindingReply;
        std::unique_ptr<IEventHandle> oldBindingReply;
        std::optional<ui64> expiredBindingCookie;
        ui32 initialPushes = 0;
        ui32 probesFromWorkingRoot = 0;
        ui32 workingRootReplies = 0;
        bool disconnected = false;
        THashMap<TActorId, std::pair<TActorId, ui64>> workingRootSubscriptions;
        bool blockProbeReplies = fault == EJoinFault::ExpiredProbe || fault == EJoinFault::UnsupportedProbe;
        std::unique_ptr<IEventHandle> oldProbeReply;
        std::optional<ui64> replayProbeCookie;

        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasQueryWorkingRoot()
                && !keepers.empty() && ev->Sender == keepers.front()) {
                ++probesFromWorkingRoot;
            }
            if (fault == EJoinFault::DisconnectBeforeBinding && type == TEvInterconnect::TEvNodeConnected::EventType
                && ev->Get<TEvInterconnect::TEvNodeConnected>()->NodeId == 1) {
                workingRootSubscriptions[ev->Recipient] = {ev->Sender, ev->Cookie};
            }
            if (ev->InterconnectSession && (ev->Sender.NodeId() == 1 || nodeId == 1) && partitioned) {
                return false;
            }
            const bool holdOperation = (fault == EJoinFault::BusyOperation && nodeId != 1)
                                       || (fault == EJoinFault::BusyWorkingRoot && nodeId == 1);
            if (holdOperation && !allowOldOperation) {
                if (!oldOperation && !ev->InterconnectSession && type == NStorage::TEvNodeConfigGather::EventType
                    && ev->Sender.NodeId() == nodeId && ev->Recipient.NodeId() == nodeId
                    && std::ranges::find(keepers, ev->Sender) != keepers.end()
                    && std::ranges::find(keepers, ev->Recipient) == keepers.end()) {
                    oldOperation = ev->Recipient;
                }
                if (oldOperation && ev->Recipient == oldOperation) {
                    oldOperationEvents.push_back(std::move(ev));
                    return false;
                }
            }
            if (type == NStorage::TEvNodeConfigInvokeOnRootResult::EventType
                && std::ranges::find(keepers, ev->Recipient) != keepers.end()
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record.HasScepter()) {
                ++workingRootReplies;
                if (blockProbeReplies) {
                    if (fault == EJoinFault::UnsupportedProbe) {
                        // Older binaries include their scepter even when the request is unknown.
                        ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record.SetStatus(
                            NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
                    } else if (replayProbeCookie == ev->Cookie) {
                        replayProbeCookie.reset();
                    } else {
                        if (!oldProbeReply) {
                            oldProbeReply = std::move(ev);
                        }
                        return false;
                    }
                }
            }
            if (type == NStorage::TEvNodeConfigPush::EventType && nodeId == 1
                && ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetInitial()) {
                ++initialPushes;
                if (fault == EJoinFault::DisconnectBeforeBinding && !disconnected) {
                    disconnected = true;
                    const auto& [sessionId, cookie] = workingRootSubscriptions.at(ev->Sender);
                    // The mock cannot reconnect a poisoned session.
                    runtime.Send(new IEventHandle(ev->Sender, sessionId, new TEvInterconnect::TEvNodeDisconnected(1),
                                                  0, cookie), ev->Sender.NodeId());
                    return false;
                }
                if (blockBindingReplies && expiredBindingCookie && ev->Cookie != *expiredBindingCookie) {
                    blockBindingReplies = false;
                }
            }
            if (blockBindingReplies && type == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Sender.NodeId() == 1) {
                if (!oldBindingReply) {
                    expiredBindingCookie = ev->Cookie;
                    oldBindingReply = std::move(ev);
                }
                return false;
            }
            return true;
        };
        for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
            keepers.push_back(RegisterKeeper(runtime, nodeId == 1 ? working : initial, nodeId, state));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return FindConvergedRoot(runtime, {keepers[0]}) == 1
                   && FindConvergedRoot(runtime, {keepers[1], keepers[2]}).has_value()
                   && ((fault != EJoinFault::BusyOperation && fault != EJoinFault::BusyWorkingRoot) || oldOperation);
        }));
        const auto before = QueryKeeper(runtime, keepers[0], true);
        UNIT_ASSERT(before.HasScepter());
        const auto otherRoot = *FindConvergedRoot(runtime, {keepers[1], keepers[2]});
        UNIT_ASSERT(!QueryKeeper(runtime, keepers[otherRoot - 1], true).HasScepter());
        partitioned = false;
        if (blockProbeReplies) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return workingRootReplies != 0; }));
            const auto beforeProbe = QueryKeeper(runtime, keepers[otherRoot - 1]);
            CheckStaysConverged(runtime, {keepers[1], keepers[2]});
            if (oldProbeReply) {
                replayProbeCookie = oldProbeReply->Cookie;
                runtime.Send(oldProbeReply.release(), otherRoot);
                CheckStaysConverged(runtime, {keepers[1], keepers[2]});
                UNIT_ASSERT(!replayProbeCookie);
            }
            const auto afterProbe = QueryKeeper(runtime, keepers[otherRoot - 1]);
            UNIT_ASSERT(beforeProbe.HasScepter() && afterProbe.HasScepter());
            UNIT_ASSERT_VALUES_EQUAL(afterProbe.GetScepter().GetId(), beforeProbe.GetScepter().GetId());
            blockProbeReplies = false;
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }));
        UNIT_ASSERT(workingRootReplies);
        UNIT_ASSERT_VALUES_EQUAL(probesFromWorkingRoot, 0u);
        UNIT_ASSERT(initialPushes >= (fault == EJoinFault::LostBindingReply
                                      || fault == EJoinFault::DisconnectBeforeBinding ? 2u : 1u));

        // Deliver a retired reply before its queued abort.
        if (fault == EJoinFault::BusyOperation) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return oldOperationEvents.size() >= 2; }));
        }
        allowOldOperation = true;
        for (auto& ev : oldOperationEvents) {
            runtime.Send(ev.release(), oldOperation.NodeId());
        }
        if (oldBindingReply) {
            const ui32 recipientNodeId = oldBindingReply->Recipient.NodeId();
            runtime.Send(oldBindingReply.release(), recipientNodeId);
        }
        if (fault == EJoinFault::ObsoleteDisconnect) {
            const TActorId keeperId = keepers[otherRoot - 1];
            runtime.WrapInActorContext(keeperId, [&](IActor*) {
                // A failed subscription reports from the proxy, not the established session.
                TActivationContext::Send(new IEventHandle(keeperId, TActivationContext::InterconnectProxy(1),
                                                          new TEvInterconnect::TEvNodeDisconnected(1)));
            });
        }
        CheckStaysConverged(runtime, keepers);
        const auto after = QueryKeeper(runtime, keepers[0]);
        UNIT_ASSERT(after.HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(after.GetScepter().GetId(), before.GetScepter().GetId());
        UNIT_ASSERT_VALUES_EQUAL(after.GetQueryConfig().GetConfig().GetFingerprint(), working.GetFingerprint());
    }

    Y_UNIT_TEST(InitialConfigRootJoinsWorkingRoot) {
        CheckJoinWorkingRoot(EJoinFault::None);
    }

    Y_UNIT_TEST(BusyRootJoinsAndIgnoresRetiredOperationReply) {
        CheckJoinWorkingRoot(EJoinFault::BusyOperation);
    }

    Y_UNIT_TEST(WorkingRootAnswersDiscoveryWhileBusy) {
        CheckJoinWorkingRoot(EJoinFault::BusyWorkingRoot);
    }

    Y_UNIT_TEST(ExpiredRootProbeReplyDoesNotReleaseRoot) {
        CheckJoinWorkingRoot(EJoinFault::ExpiredProbe);
    }

    Y_UNIT_TEST(UnknownProbeResponseDoesNotReleaseRoot) {
        CheckJoinWorkingRoot(EJoinFault::UnsupportedProbe);
    }

    Y_UNIT_TEST(LostBindingReplyIsRetriedAndLateReplyIsIgnored) {
        CheckJoinWorkingRoot(EJoinFault::LostBindingReply);
    }

    Y_UNIT_TEST(WorkingRootDisconnectsBeforeBinding) {
        CheckJoinWorkingRoot(EJoinFault::DisconnectBeforeBinding);
    }

    Y_UNIT_TEST(ExpiredSubscriptionFailureDoesNotBreakBinding) {
        CheckJoinWorkingRoot(EJoinFault::ObsoleteDisconnect);
    }

    enum class ERestartOrder {
        ConfigMajorityFirst,
        StorageQuorumFirst,
        Simultaneous,
        Partitioned,
        PartitionAfterCollection,
    };

    void CheckPartialBootstrapRestart(ERestartOrder order, TDuration bindFailureDelay = TDuration::Seconds(5),
                                      bool unevenDrives = false) {
        auto config = MakeStorageConfig(3, 0);
        auto *selfManagement = config.MutableSelfManagementConfig();
        selfManagement->SetEnabled(true);
        selfManagement->SetAutomaticBootstrap(false);
        selfManagement->SetErasureSpecies("none");
        selfManagement->SetPDiskType(NKikimrBlobStorage::EPDiskType::ROT);
        for (ui32 nodeId : {2, 3}) {
            config.MutableBlobStorageConfig()->MutableDefineHostConfig(nodeId - 1)->MutableDrive(0)->SetType(
                NKikimrBlobStorage::EPDiskType::SSD);
        }
        if (unevenDrives) {
            for (auto& node : *config.MutableAllNodes()) {
                node.MutableLocation()->SetDataCenter("dc-1");
            }
            auto *hostConfig = config.MutableBlobStorageConfig()->MutableDefineHostConfig(0);
            for (ui32 diskId : {2, 3}) {
                auto *drive = hostConfig->AddDrive();
                drive->SetPath("/dev/disk1_" + std::to_string(diskId));
                drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
            }
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        const TString startupConfigYaml = R"(metadata:
  kind: MainConfig
  version: 0
  cluster: ""
config: {}
allowed_labels: {}
selector_config: []
)";
        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        const auto metadata = [&](ui32 nodeId) -> const NKikimrBlobStorage::TPDiskMetadataRecord& {
            return state.Metadata.at(std::pair<ui32, TString>{nodeId, "/dev/disk" + std::to_string(nodeId) + "_1"});
        };

        {
            TTestActorSystem runtime(3);
            runtime.Start();
            Y_DEFER { runtime.Stop(); };

            std::vector<TActorId> keeperIds;
            for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
                keeperIds.push_back(RegisterKeeper(runtime, config, nodeId, state, startupConfigYaml));
            }
            UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keeperIds) == 1; }));

            runtime.FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
                return ev->GetTypeRewrite() != NStorage::TEvNodeConfigReversePush::EventType
                       || !ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.HasCommittedStorageConfig();
            };
            BootstrapCluster(runtime, keeperIds.front());
            UNIT_ASSERT(SimUntil(runtime, [&] {
                return state.Metadata.size() == (unevenDrives ? 5 : 3) && metadata(1).HasCommittedStorageConfig();
            }));

            const auto& committed = metadata(1).GetCommittedStorageConfig();
            UNIT_ASSERT_VALUES_EQUAL(committed.GetGeneration(), 1u);
            UNIT_ASSERT(HasQuorum(committed, {1}));
            for (ui32 nodeId : {2, 3}) {
                UNIT_ASSERT(!metadata(nodeId).HasCommittedStorageConfig());
                UNIT_ASSERT_VALUES_EQUAL(metadata(nodeId).GetProposedStorageConfig().GetGeneration(), 1u);
            }
        }

        state.RequiredRootNodeId = 0;
        TTestActorSystem runtime(3);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keeperIds;
        THashMap<TActorId, ui32> peerByProxy;
        THashSet<ui32> stoppedNodes;
        auto registerNode = [&](ui32 nodeId) {
            stoppedNodes.erase(nodeId);
            keeperIds.push_back(RegisterKeeper(runtime, config, nodeId, state, startupConfigYaml));
            runtime.WrapInActorContext(keeperIds.back(), [&](IActor*) {
                for (ui32 peerNodeId = 1; peerNodeId <= 3; ++peerNodeId) {
                    if (peerNodeId != nodeId) {
                        peerByProxy.emplace(TActivationContext::InterconnectProxy(peerNodeId), peerNodeId);
                    }
                }
            });
        };

        bool partitioned = order == ERestartOrder::Partitioned;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Subscribe) {
                const auto it = peerByProxy.find(ev->GetRecipientRewrite());
                if (it != peerByProxy.end()
                    && (stoppedNodes.contains(it->second) || (partitioned && (nodeId == 1 || it->second == 1)))) {
                    ScheduleSubscriptionFailure(runtime, nodeId, *ev, it->second, bindFailureDelay);
                    return false;
                }
            }
            return true;
        };

        const auto describeRoots = [&] {
            TStringBuilder result;
            for (TActorId id : keeperIds) {
                result << " " << id.NodeId() << "->" << FindConvergedRoot(runtime, {id}).value_or(0);
            }
            return TString(result);
        };
        TString rootsBeforeHealing;
        if (order == ERestartOrder::ConfigMajorityFirst) {
            runtime.StopNode(1);
            stoppedNodes.insert(1);
            registerNode(2);
            registerNode(3);
            UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keeperIds).has_value(); },
                                 TDuration::Seconds(30), 2));
            runtime.StartNode(1);
            registerNode(1);
        } else if (order == ERestartOrder::StorageQuorumFirst) {
            runtime.StopNode(2);
            runtime.StopNode(3);
            stoppedNodes = {2, 3};
            registerNode(1);
            SimUntil(runtime, [] { return false; });
            for (ui32 nodeId : {2, 3}) {
                runtime.StartNode(nodeId);
                registerNode(nodeId);
            }
        } else if (order == ERestartOrder::PartitionAfterCollection) {
            runtime.StopNode(3);
            stoppedNodes.insert(3);
            state.RequiredRootNodeId = 1;
            registerNode(1);
            registerNode(2);
            UNIT_ASSERT(SimUntil(runtime, [&] {
                return FindConvergedRoot(runtime, keeperIds) == 1 && metadata(2).HasCommittedStorageConfig();
            }));
            CheckStaysConverged(runtime, keeperIds);

            partitioned = true;
            state.RequiredRootNodeId = 3;
            runtime.WrapInActorContext(keeperIds.front(), [&](IActor*) {
                TActivationContext::Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0,
                                                          TActivationContext::InterconnectProxy(2), {}, nullptr, 0));
            });
            runtime.StartNode(3);
            registerNode(3);
            UNIT_ASSERT(SimUntil(runtime, [&] {
                return FindConvergedRoot(runtime, {keeperIds[1], keeperIds[2]}) == 3;
            }));
            rootsBeforeHealing = describeRoots();
            partitioned = false;
            state.RequiredRootNodeId = 0;
        } else {
            for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
                registerNode(nodeId);
            }
            if (partitioned) {
                SimUntil(runtime, [] { return false; });
                rootsBeforeHealing = describeRoots();
                partitioned = false;
            }
        }
        const bool converged = SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keeperIds).has_value(); },
                                        TDuration::Minutes(5));
        UNIT_ASSERT_C(converged,
                      "nodes with committed and proposed bootstrap configs did not join the same binding tree"
                      << "; bindFailureDelay=" << bindFailureDelay
                      << "; before healing:" << rootsBeforeHealing << "; after healing:" << describeRoots());

        const TString fingerprint = metadata(1).GetCommittedStorageConfig().GetFingerprint();
        const ui32 rootNodeId = *FindConvergedRoot(runtime, keeperIds);
        const auto root = std::ranges::find_if(keeperIds, [&](TActorId id) { return id.NodeId() == rootNodeId; });
        BootstrapCluster(runtime, *root);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(keeperIds, [&](TActorId id) {
                return metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() == fingerprint;
            });
        }), "bootstrap configuration was not recovered on all nodes");
        CheckStaysConverged(runtime, keeperIds);
    }

    Y_UNIT_TEST(PartialBootstrapRestartConvergesThroughMailbox) {
        CheckPartialBootstrapRestart(ERestartOrder::ConfigMajorityFirst);
    }

    Y_UNIT_TEST(PartialBootstrapStorageQuorumStartsFirst) {
        CheckPartialBootstrapRestart(ERestartOrder::StorageQuorumFirst);
    }

    Y_UNIT_TEST(PartialBootstrapStorageQuorumStartsFirstWithFastFailures) {
        CheckPartialBootstrapRestart(ERestartOrder::StorageQuorumFirst, TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(PartialBootstrapSimultaneousRestart) {
        CheckPartialBootstrapRestart(ERestartOrder::Simultaneous);
    }

    Y_UNIT_TEST(PartialBootstrapPartitionHeals) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned);
    }

    Y_UNIT_TEST(PartialBootstrapPartitionHealsAfterFastFailures) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned, TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(PartialBootstrapUnevenDrivesPartitionHeals) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned, TDuration::MilliSeconds(10), true);
    }

    Y_UNIT_TEST(PartialBootstrapPartitionAfterSuccessfulCollectionHeals) {
        CheckPartialBootstrapRestart(ERestartOrder::PartitionAfterCollection, TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(PartialBootstrapPartitionHealsAfter500msFailures) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned, TDuration::MilliSeconds(500));
    }

    Y_UNIT_TEST(PartialBootstrapPartitionHealsAfter999msFailures) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned, TDuration::MilliSeconds(999));
    }

    Y_UNIT_TEST(PartialBootstrapPartitionHealsAfter1000msFailures) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartialBootstrapPartitionHealsAfter1001msFailures) {
        CheckPartialBootstrapRestart(ERestartOrder::Partitioned, TDuration::MilliSeconds(1001));
    }

    Y_UNIT_TEST(BootstrapWritesKnownEmptyDisk) {
        auto initial = MakeStorageConfig(1, 0, {1});
        initial.MutableSelfManagementConfig()->SetEnabled(true);
        initial.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        auto *drive = initial.MutableBlobStorageConfig()->MutableDefineHostConfig(0)->AddDrive();
        drive->SetPath("/dev/disk1_2");
        drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&initial);
        auto committed = initial;
        committed.SetGeneration(1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&committed);
        TNodeWardenTestState state;
        state.Metadata[std::pair<ui32, TString>{1, "/dev/disk1_1"}].MutableCommittedStorageConfig()->CopyFrom(committed);
        const auto emptyKey = std::pair<ui32, TString>{1, "/dev/disk1_2"};

        TTestActorSystem runtime(1);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        RegisterKeeper(runtime, initial, 1, state);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            const auto it = state.Metadata.find(emptyKey);
            return it != state.Metadata.end() &&
                it->second.GetCommittedStorageConfig().GetFingerprint() == committed.GetFingerprint();
        }), "Bootstrap did not write the local commit to a disk successfully read as empty");
    }

    Y_UNIT_TEST(BootstrapReadErrorsAllowQuorumRecovery) {
        auto initial = MakeStorageConfig(9, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9},
            TBlobStorageGroupType::ErasureMirror3dc);
        initial.MutableSelfManagementConfig()->SetEnabled(true);
        initial.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        auto *bs = initial.MutableBlobStorageConfig();
        auto *serviceSet = bs->MutableServiceSet();
        auto *group = serviceSet->MutableGroups(0);
        const auto domains = group->GetRings(0);
        group->ClearRings();
        for (ui32 dc = 0; dc < 3; ++dc) {
            auto *ring = group->AddRings();
            for (ui32 domain = 0; domain < 3; ++domain) {
                const ui32 index = dc * 3 + domain;
                ring->AddFailDomains()->CopyFrom(domains.GetFailDomains(index));
                auto *id = serviceSet->MutableVDisks(index)->MutableVDiskID();
                id->SetRing(dc);
                id->SetDomain(domain);
                initial.MutableAllNodes(index)->MutableLocation()->SetDataCenter("dc-" + std::to_string(dc));
                for (ui32 disk : {2, 3}) {
                    auto *drive = bs->MutableDefineHostConfig(index)->AddDrive();
                    drive->SetPath("/dev/disk" + std::to_string(index + 1) + "_" + std::to_string(disk));
                    drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
                }
            }
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&initial);
        auto older = initial;
        older.SetGeneration(1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&older);
        auto newer = older;
        newer.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&newer);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        std::vector<NStorage::TSuccessfulDisk> available;
        std::vector<NStorage::TSuccessfulDisk> newerDisks;
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            for (ui32 disk = 1; disk <= 3; ++disk) {
                const TString path = "/dev/disk" + std::to_string(nodeId) + "_" + std::to_string(disk);
                state.Metadata[std::pair<ui32, TString>{nodeId, path}].MutableCommittedStorageConfig()->CopyFrom(
                    nodeId == 9 && disk != 3 ? older : newer);
                if (disk != 3) {
                    available.emplace_back(NStorage::TNodeIdentifier(newer.GetAllNodes(nodeId - 1)),
                        path, nodeId * 1000 + disk);
                    if (nodeId != 9) {
                        newerDisks.push_back(available.back());
                    }
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(available.size(), 18);
        const auto nwConfig = MakeNodeWardenConfig(newer);
        UNIT_ASSERT(NStorage::HasConfigQuorum(newer, available, {}, {}, *nwConfig, false));
        UNIT_ASSERT(NStorage::HasConfigQuorum(newer, newerDisks, {}, {}, *nwConfig, false));

        TTestActorSystem runtime(9);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        bool allowCollection = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> heldScatters;
        std::array<ui32, 9> failedReads{};
        ui32 writesBeforeCollection = 0;
        bool downgraded = false;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!allowCollection && ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType) {
                heldScatters.emplace_back(nodeId, std::move(ev));
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType &&
                    ev->Get<NStorage::TEvNodeWardenReadMetadata>()->Path.EndsWith("_3")) {
                ++failedReads.at(nodeId - 1);
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new NStorage::TEvNodeWardenReadMetadataResult(std::nullopt,
                        NPDisk::EPDiskMetadataOutcome::ERROR, {}), 0, ev->Cookie), nodeId);
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                writesBeforeCollection += !allowCollection;
                const auto *msg = ev->Get<NStorage::TEvNodeWardenWriteMetadata>();
                const auto& previous = state.Metadata.at(std::pair<ui32, TString>{nodeId, msg->Path});
                downgraded |= msg->Record.GetCommittedStorageConfig().GetGeneration() <
                    previous.GetCommittedStorageConfig().GetGeneration();
                if (msg->Path.EndsWith("_3")) {
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                        new NStorage::TEvNodeWardenWriteMetadataResult(std::nullopt,
                            NPDisk::EPDiskMetadataOutcome::ERROR), 0, ev->Cookie), nodeId);
                    return false;
                }
            }
            return true;
        };
        std::vector<TActorId> keepers;
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            keepers.push_back(RegisterKeeper(runtime, initial, nodeId, state));
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }),
            "An unreadable non-static disk on each node prevented the binding tree from forming");
        UNIT_ASSERT(std::ranges::all_of(failedReads, [](ui32 count) { return count != 0; }));
        UNIT_ASSERT_VALUES_EQUAL(writesBeforeCollection, 0);
        UNIT_ASSERT(!downgraded);

        // No failed read is repaired. Recovery must use the remaining disk and static-group quorum.
        allowCollection = true;
        for (auto& [nodeId, ev] : heldScatters) {
            runtime.Send(ev.release(), nodeId);
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(state.Metadata, [&](const auto& item) {
                return item.second.GetCommittedStorageConfig().GetFingerprint() == newer.GetFingerprint();
            });
        }, TDuration::Minutes(2)), "The available quorum did not restore the older node's configuration");
        UNIT_ASSERT(!downgraded);
        QueryKeeper(runtime, keepers.front(), true); // the root reached its working state
        CheckStaysConverged(runtime, keepers);
        UNIT_ASSERT(std::ranges::all_of(failedReads, [](ui32 count) { return count > 1; }));
        UNIT_ASSERT(!downgraded);
    }

    Y_UNIT_TEST(BootstrapReadErrorDoesNotOverwriteNewerConfig) {
        auto initial = MakeStorageConfig(1, 0, {1});
        initial.MutableSelfManagementConfig()->SetEnabled(true);
        initial.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        auto *drive = initial.MutableBlobStorageConfig()->MutableDefineHostConfig(0)->AddDrive();
        drive->SetPath("/dev/disk1_2");
        drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&initial);
        auto older = initial;
        older.SetGeneration(1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&older);
        auto newer = older;
        newer.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&newer);
        TNodeWardenTestState state;
        const auto oldKey = std::pair<ui32, TString>{1, "/dev/disk1_1"};
        const auto newKey = std::pair<ui32, TString>{1, "/dev/disk1_2"};
        state.Metadata[oldKey].MutableCommittedStorageConfig()->CopyFrom(older);
        // A partially persisted commit follows a successful proposal quorum.
        state.Metadata[oldKey].MutableProposedStorageConfig()->CopyFrom(newer);
        state.Metadata[newKey].MutableCommittedStorageConfig()->CopyFrom(newer);

        TTestActorSystem runtime(1);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        bool failRead = true;
        ui32 failedReads = 0;
        ui32 writes = 0;
        bool downgraded = false;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (failRead && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType &&
                    ev->Get<NStorage::TEvNodeWardenReadMetadata>()->Path == newKey.second) {
                ++failedReads;
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new NStorage::TEvNodeWardenReadMetadataResult(std::nullopt,
                        NPDisk::EPDiskMetadataOutcome::ERROR, {}), 0, ev->Cookie), nodeId);
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                ++writes;
                const auto *msg = ev->Get<NStorage::TEvNodeWardenWriteMetadata>();
                const auto it = state.Metadata.find(std::pair<ui32, TString>{1, msg->Path});
                if (it != state.Metadata.end() && msg->Record.GetCommittedStorageConfig().GetGeneration() <
                        it->second.GetCommittedStorageConfig().GetGeneration()) {
                    downgraded = true;
                }
            }
            return true;
        };
        RegisterKeeper(runtime, initial, 1, state);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(65));
        UNIT_ASSERT(failedReads);
        UNIT_ASSERT_C(!downgraded, "Bootstrap wrote an older configuration over a newer local commit");
        UNIT_ASSERT_VALUES_EQUAL_C(state.Metadata.at(newKey).GetCommittedStorageConfig().GetGeneration(), 2,
            "Bootstrap overwrote the unread newer committed configuration");
        UNIT_ASSERT_VALUES_EQUAL(writes, 0);
        UNIT_ASSERT_C(failedReads > 1, "Quorum collection did not retry the failed metadata read");
        failRead = false;
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return state.Metadata.at(oldKey).GetCommittedStorageConfig().GetGeneration() == 2;
        }), "Quorum recovery did not finish the partial commit after the read succeeded");
        UNIT_ASSERT_VALUES_EQUAL(state.Metadata.at(newKey).GetCommittedStorageConfig().GetGeneration(), 2);
        UNIT_ASSERT(writes);
        runtime.FilterFunction = nullptr;
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
        TNodeWardenTestState state{9, 10, 11, 12, 13, 14, 15, 16, 17};
        const ui32 rootNodeId = RunUntilConverged(config, state, TDuration::Minutes(10), true);
        UNIT_ASSERT(state.SawCompleteMobileBinding);
        UNIT_ASSERT(state.SawScatter);
        UNIT_ASSERT(1 <= rootNodeId && rootNodeId <= 8);
    }
}

Y_UNIT_TEST_SUITE(TDistconfGenerateConfigTest) {

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateSimpleStateStorage(ui32 nodes, std::unordered_set<ui32> usedNodes = {}, ui32 overrideReplicasInRingCount = 0, ui32 overrideRingsCount = 0, ui32 replicasSpecificVolume = 200, std::unordered_set<ui32> nodesToUse = {}, bool *goodConfigOut = nullptr, const NKikimrConfig::TDomainsConfig::TStateStorage& oldSS = {}, bool automaticManagement = true) {
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(nodes)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        bool goodConfig = keeper.GenerateStateStorageConfig(&ss, config, usedNodes, nodesToUse, oldSS, automaticManagement, overrideReplicasInRingCount, overrideRingsCount, replicasSpecificVolume);
        if (goodConfigOut) {
            *goodConfigOut = goodConfig;
        }
        return ss;
    }

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateDCStateStorage(
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
        NKikimrConfig::TDomainsConfig::TStateStorage oldSS;
        if (!oldConfig.empty()) {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(oldNToSelect);
            for (ui32 node : oldConfig) {
                auto* ssRing = rg->AddRing();
                ssRing->AddNode(node);
            }
        }
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        for (auto [nodeId, state] : nodesState) {
            keeper.SelfHealNodesState[nodeId] = state;
        }
        bool goodConfig = keeper.GenerateStateStorageConfig(&ss, config, usedNodes, nodesToUse, oldSS, automaticManagement, overrideReplicasInRingCount, overrideRingsCount, replicasSpecificVolume);
        if (goodConfigOut) {
            *goodConfigOut = goodConfig;
        }
        return ss;
    }

    void CheckStateStorage(const NKikimrConfig::TDomainsConfig::TStateStorage& ss, ui32 nToSelect, const std::unordered_set<ui32>& nodes) {
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

    void CheckStateStorage2(const NKikimrConfig::TDomainsConfig::TStateStorage& ss, std::string expected) {
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
        NKikimrConfig::TDomainsConfig::TStateStorage oldSS;
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
        NKikimrConfig::TDomainsConfig::TStateStorage oldSS;
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
        NKikimrConfig::TDomainsConfig::TStateStorage oldSS;
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
        NKikimrConfig::TDomainsConfig::TStateStorage oldSS;
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
        NKikimrConfig::TDomainsConfig::TStateStorage ssOut;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(8)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        std::unordered_set<ui32> refUsedNodes;
        bool refGoodConfig = keeper.GenerateStateStorageConfig(&ssOut, config, refUsedNodes, {}, oldSS,
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
        NKikimrConfig::TDomainsConfig::TStateStorage oldStateStorage;
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
        NKikimrConfig::TDomainsConfig::TStateStorage stateStorageOut;
        bool goodConfig1 = keeper.GenerateStateStorageConfig(&stateStorageOut, config, usedNodes, {},
            oldStateStorage, /*automaticManagement=*/false, 0, 0, 200);
        UNIT_ASSERT(goodConfig1);
        UNIT_ASSERT(usedNodes.contains(1));
        UNIT_ASSERT(usedNodes.contains(2));
        UNIT_ASSERT(usedNodes.contains(3));

        // Step 2: StateStorageBoard generation with automatic management enabled, sharing usedNodes.
        NKikimrConfig::TDomainsConfig::TStateStorage boardOut;
        NKikimrConfig::TDomainsConfig::TStateStorage emptyOldBoard;
        bool goodConfig2 = keeper.GenerateStateStorageConfig(&boardOut, config, usedNodes, {},
            emptyOldBoard, /*automaticManagement=*/true, 0, 0, 200);
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

        ui32 GetGroupVDiskNode() const {
            const auto& group = Config.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
            return group.GetRings(0).GetFailDomains(0).GetVDiskLocations(0).GetNodeID();
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
    };

    NKikimrBlobStorage::TGroupGeometry Geometry(ui32 numFailDomains) {
        NKikimrBlobStorage::TGroupGeometry g;
        g.SetNumFailRealms(1);
        g.SetNumFailDomainsPerFailRealm(numFailDomains);
        g.SetNumVDisksPerFailDomain(1);
        return g;
    }

    void Reallocate(TSetup& s, const NProtoBuf::RepeatedField<ui32>& allowedNodeIds, bool applyNodeAllowList,
                    i32 erasureSpecies = TBlobStorageGroupType::ErasureNone, ui32 numFailDomains = 1) {
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
        replacedDisks.emplace(TVDiskIdShort(0, 0, 0), NBsController::TPDiskId());
        NBsController::TGroupMapper::TForbiddenPDisks forbid;
        keeper.AllocateStaticGroup(&s.Config, TGroupId::FromValue(0), /*groupGeneration=*/ 2,
            TBlobStorageGroupType((TBlobStorageGroupType::EErasureSpecies)erasureSpecies), Geometry(numFailDomains),
            /*pdiskFilters=*/ {}, NKikimrBlobStorage::EPDiskType::ROT, replacedDisks, forbid,
            /*requiredSpace=*/ 0, &s.BaseConfig, /*convertToDonor=*/ false, /*ignoreVSlotQuotaCheck=*/ true,
            /*isSelfHealReasonDecommit=*/ false, TBridgePileId(), std::nullopt, allowedNodeIds, applyNodeAllowList);
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
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskNode(), 3u);
    }

    Y_UNIT_TEST(RespectsNodeAllowListAnotherNode) {
        TSetup s = MakeSetup();
        Reallocate(s, NodeIds({4}), /*applyNodeAllowList=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskNode(), 4u);
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

    Y_UNIT_TEST(Block42KeepsExistingVDisksOnNonAllowedNodes) {
        TSetup s = MakeBlock42Setup();
        Reallocate(s, NodeIds({10}), /*applyNodeAllowList=*/ true, TBlobStorageGroupType::Erasure4Plus2Block, /*numFailDomains=*/ 8);

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
            Reallocate(s, NodeIds({2}), /*applyNodeAllowList=*/ true, TBlobStorageGroupType::Erasure4Plus2Block, /*numFailDomains=*/ 8),
            NKikimr::NStorage::TDistributedConfigKeeper::TExConfigError);
    }
}

}
}
