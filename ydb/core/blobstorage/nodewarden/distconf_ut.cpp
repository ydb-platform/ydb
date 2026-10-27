#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/core/util/random.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/json/json_reader.h>
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
        const bool mirror3dc = erasureSpecies == TBlobStorageGroupType::ErasureMirror3dc;

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

            if (mirror3dc && domainIdx) {
                ring = group->AddRings();
            }
            const ui32 domainCount = mirror3dc ? 3 : 1;
            for (ui32 index = 0; index < domainCount; ++index) {
                auto *vdisk = serviceSet->AddVDisks();
                auto *vdiskId = vdisk->MutableVDiskID();
                vdiskId->SetGroupID(groupId);
                vdiskId->SetGroupGeneration(1);
                vdiskId->SetRing(mirror3dc ? domainIdx : 0);
                vdiskId->SetDomain(mirror3dc ? index : domainIdx);
                vdiskId->SetVDisk(0);
                const auto fillLocation = [&](auto *location) {
                    location->SetNodeID(nodeId);
                    location->SetPDiskID(pdiskId);
                    location->SetVDiskSlotID(index);
                    location->SetPDiskGuid(pdiskGuid);
                };
                fillLocation(vdisk->MutableVDiskLocation());
                fillLocation(ring->AddFailDomains()->AddVDiskLocations());
            }
        }
    }

    TIntrusivePtr<TNodeWardenConfig> MakeNodeWardenConfig(const NKikimrBlobStorage::TStorageConfig& storageConfig,
            const NKikimrConfig::TBridgeConfig *bridgeConfig = nullptr) {
        auto config = MakeIntrusive<TNodeWardenConfig>();
        if (bridgeConfig) {
            config->BridgeConfig = std::make_unique<NKikimrConfig::TBridgeConfig>(*bridgeConfig);
        }
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

    struct TNodeWardenTestState {
        THashSet<ui32> MobileNodes;
        THashMap<std::pair<ui32, TString>, NKikimrBlobStorage::TPDiskMetadataRecord> Metadata;
        THashMap<ui32, std::shared_ptr<const NKikimrBlobStorage::TStorageConfig>> NodeWardenConfigs;
        THashMap<ui32, ui32> RequiredParentNodeIds;
        ui32 RequiredRootNodeId = 0;
        bool SawCompleteMobileBinding = false;
        bool SawScatter = false;

        TNodeWardenTestState() = default;

        explicit TNodeWardenTestState(std::initializer_list<ui32> mobileNodes)
            : MobileNodes(mobileNodes.begin(), mobileNodes.end())
        {}

        bool ShouldRejectBinding(ui32 senderNodeId, ui32 recipientNodeId,
                                 const NKikimrBlobStorage::TEvNodeConfigPush& record) {
            if (const auto it = RequiredParentNodeIds.find(senderNodeId); it != RequiredParentNodeIds.end()) {
                return recipientNodeId != it->second;
            }
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
                } else if (type == TEvNodeWardenStorageConfig::EventType) {
                    State.NodeWardenConfigs[SelfId().NodeId()] = ev->Get<TEvNodeWardenStorageConfig>()->Config;
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
        return NStorage::HasNodeQuorum(config, successfulNodes, TBridgePileId(), *nodeWardenConfig, nullptr, true);
    }

    TActorId RegisterKeeper(TTestActorSystem& runtime, const NKikimrBlobStorage::TStorageConfig& config, ui32 nodeId,
                            TNodeWardenTestState& state, const TString& startupConfigYaml = {},
                            const NKikimrConfig::TBridgeConfig *bridgeConfig = nullptr,
                            const NKikimrConfig::TSelfManagementConfig *selfManagementConfig = nullptr) {
        auto nodeWardenConfig = MakeNodeWardenConfig(config, bridgeConfig);
        nodeWardenConfig->StartupConfigYaml = startupConfigYaml;
        if (selfManagementConfig) {
            nodeWardenConfig->SelfManagementConfig =
                std::make_unique<NKikimrConfig::TSelfManagementConfig>(*selfManagementConfig);
        }
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

    void ReplayDelayedEvents(TTestActorSystem& runtime, std::vector<std::unique_ptr<IEventHandle>>& events) {
        for (auto& ev : events) {
            const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
            runtime.Send(ev.release(), nodeId);
        }
        events.clear();
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

    // two-pile bridged config of 4 nodes: nodes 1 and 4 are in the first pile, nodes 2 and 3 are in the second one; the
    // first pile has static group on the three drives of node 1, the second one -- on the single drive of node 3
    NKikimrBlobStorage::TStorageConfig MakeTwoPileBridgedConfig(const TString& firstPileName,
            const TString& secondPileName) {
        auto config = MakeStorageConfig(4);
        config.MutableSelfManagementConfig()->SetEnabled(true);
        for (auto& node : *config.MutableAllNodes()) {
            node.MutableLocation()->SetBridgePileName(node.GetNodeId() == 1 || node.GetNodeId() == 4
                ? firstPileName : secondPileName);
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
        config.MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(2));
        config.MutableClusterState()->SetGeneration(1);
        auto *registry = config.MutableClusterStateDetails()->MutablePileRegistry();
        registry->AddPiles()->SetName(firstPileName);
        registry->AddPiles()->SetName(secondPileName);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        return config;
    }

    // config of the cluster bridged before pile registry was introduced
    NKikimrBlobStorage::TStorageConfig MakeTwoPileBridgedConfigWithoutRegistry(const TString& firstPileName = "pile0",
            const TString& secondPileName = "pile1") {
        auto config = MakeTwoPileBridgedConfig(firstPileName, secondPileName);
        config.MutableClusterStateDetails()->ClearPileRegistry();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        return config;
    }

    // make the config committed on all the drives of MakeTwoPileBridgedConfig
    void FillTwoPileBridgedMetadata(TNodeWardenTestState& state, const NKikimrBlobStorage::TStorageConfig& config) {
        for (ui32 nodeId : {1u, 3u}) {
            state.Metadata[std::make_pair(nodeId, TString("/dev/disk" + std::to_string(nodeId) + "_1"))]
                .MutableCommittedStorageConfig()->CopyFrom(config);
        }
        for (ui32 index : {2u, 3u}) {
            state.Metadata[std::make_pair(1u, TString("/dev/disk1_" + std::to_string(index)))]
                .MutableCommittedStorageConfig()->CopyFrom(config);
        }
    }

    // keep the latest bridge info reported to NodeWarden on each node
    void TrackReportedBridgeInfo(TTestActorSystem& runtime, THashMap<ui32, TBridgeInfo::TPtr>& reported) {
        runtime.FilterFunction = [map = &reported](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType && ev->HasEvent() &&
                    ev->Recipient == MakeBlobStorageNodeWardenID(nodeId)) {
                if (auto& bridgeInfo = ev->Get<TEvNodeWardenStorageConfig>()->BridgeInfo) {
                    (*map)[nodeId] = bridgeInfo;
                }
            }
            return true;
        };
    }

    // pile registry lists piles in the order other than local BridgeConfig does; pile ids must come from the registry
    Y_UNIT_TEST(PileRegistryDefinesPileIds) {
        const auto config = MakeTwoPileBridgedConfig("pile1", "pile0");
        if (const auto error = NStorage::ValidateConfig(config)) {
            UNIT_FAIL(*error);
        }

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        FillTwoPileBridgedMetadata(state, config);
        TTestActorSystem runtime(4);
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");

        THashMap<ui32, TBridgeInfo::TPtr> reported;
        TrackReportedBridgeInfo(runtime, reported);

        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        for (ui32 nodeId : {1u, 3u}) {
            keepers.push_back(RegisterKeeper(runtime, config, nodeId, state, {}, &bridge));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }));
        UNIT_ASSERT(QueryKeeper(runtime, keepers.front()).HasScepter());

        for (ui32 nodeId : {1u, 3u}) {
            UNIT_ASSERT(reported.contains(nodeId));
            const auto& info = *reported[nodeId];
            UNIT_ASSERT_VALUES_EQUAL(info.Piles.size(), 2);
            const auto *first = info.GetPileForNode(1);
            const auto *second = info.GetPileForNode(3);
            UNIT_ASSERT(first && second);
            UNIT_ASSERT_EQUAL(first->BridgePileId, TBridgePileId::FromPileIndex(0));
            UNIT_ASSERT_VALUES_EQUAL(first->Name, "pile1");
            UNIT_ASSERT_EQUAL(second->BridgePileId, TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_VALUES_EQUAL(second->Name, "pile0");
            UNIT_ASSERT_EQUAL(info.GetPileForNode(4), first);
            UNIT_ASSERT_EQUAL(info.SelfNodePile, nodeId == 1 ? first : second);
            UNIT_ASSERT_EQUAL(info.ResolvePile("pile0"), second);
            UNIT_ASSERT(!info.ResolvePile(std::nullopt)); // no implicit pile in multi-pile cluster
            UNIT_ASSERT(!info.ImplicitPile);
        }
    }

    // interconnect session of the dynamic node, which receives events sent to the node through it
    class TSessionStub : public TActor<TSessionStub> {
        std::vector<NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush>& Pushes;

    public:
        TSessionStub(std::vector<NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush>& pushes)
            : TActor(&TThis::StateFunc)
            , Pushes(pushes)
        {}

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == NStorage::TEvNodeWardenDynamicConfigPush::EventType) {
                Pushes.push_back(ev->Get<NStorage::TEvNodeWardenDynamicConfigPush>()->Record);
            }
        }
    };

    // subscribes dynamic node to the keeper; pushes get the configs pushed to it
    TActorId SubscribeDynamicNode(TTestActorSystem& runtime, TActorId keeperId, ui32 nodeId, ui32 bridgeFeatureLevel,
            std::vector<NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush>& pushes) {
        // the test system delivers events to the recipient of the forwarded event, so the stub is also the sender
        const TActorId sessionId = runtime.Register(new TSessionStub(pushes), nodeId);
        NKikimrBlobStorage::TEvNodeWardenDynamicConfigSubscribe record;
        record.SetBridgeCapable(true);
        // older versions report only PileListChangeCapable
        record.SetPileListChangeCapable(bridgeFeatureLevel >= NStorage::BridgeFeatureLevelPileList);
        if (bridgeFeatureLevel > NStorage::BridgeFeatureLevelPileList) {
            record.SetBridgeFeatureLevel(bridgeFeatureLevel);
        }
        TString data;
        UNIT_ASSERT(record.SerializeToString(&data));
        runtime.Send(new IEventHandle(sessionId, NStorage::TEvNodeWardenDynamicConfigSubscribe::EventType, 0,
            keeperId, sessionId, MakeIntrusive<TEventSerializedData>(std::move(data), TEventSerializationInfo{}),
            0, {}, {}), nodeId);
        UNIT_ASSERT(SimUntil(runtime, [&] { return !pushes.empty(); }));
        return sessionId;
    }

    // dynamic nodes of older versions get no configuration once it requires the bridge features they don't support: the
    // ones taking pile ids from positions in their BridgeConfig once the list of piles has been changed, the ones
    // supporting only changes of the list of piles once a pile has been removed
    Y_UNIT_TEST_TWIN(OlderDynamicNodeGetsNoConfig, nonUniformStorage) {
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        auto *registry = config.MutableClusterStateDetails()->MutablePileRegistry();
        registry->SetPileListChanged(true);
        const ui32 requiredLevel = nonUniformStorage ? NStorage::BridgeFeatureLevelNonUniformStorage
            : NStorage::BridgeFeatureLevelPileOps;
        registry->SetRequiredFeatureLevel(requiredLevel);
        config.MutableClusterStateDetails()->SetNonUniformStorageEnabled(nonUniformStorage);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        FillTwoPileBridgedMetadata(state, config);
        TTestActorSystem runtime(5 + NStorage::BridgeFeatureLevel); // nodes from 5 are dynamic ones
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        for (ui32 nodeId : {1u, 3u}) {
            keepers.push_back(RegisterKeeper(runtime, config, nodeId, state, {}, &bridge));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }));

        for (ui32 level = 0; level <= NStorage::BridgeFeatureLevel; ++level) {
            std::vector<NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush> pushes;
            SubscribeDynamicNode(runtime, keepers.front(), 5 + level, level, pushes);
            UNIT_ASSERT_VALUES_EQUAL_C(pushes.front().HasConfig(), level >= requiredLevel, level);
        }
    }

    Y_UNIT_TEST(NonUniformStorageActivationValidation) {
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        NKikimrConfig::TBridgeConfig bridge;
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        bridge.SetEnableNonUniformStorage(true);
        bool pileListChanged = false;
        auto pending = config;
        pending.MutableClusterStateDetails()->SetBscConversionPending(true);
        auto error = NStorage::UpdatePileRegistry(bridge, &pending, &bridge, &bridge, &pileListChanged);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "conversion to complete");

        error = NStorage::UpdatePileRegistry(bridge, &config, &bridge, &bridge, &pileListChanged);
        UNIT_ASSERT_C(!error, error.value_or(TString()));
        UNIT_ASSERT(!pileListChanged);
        UNIT_ASSERT(!NStorage::ValidateConfig(config));
        auto proposed = config;
        proposed.SetGeneration(config.GetGeneration() + 1);
        proposed.MutableClusterStateDetails()->SetNonUniformStorageEnabled(false);
        UNIT_ASSERT(NStorage::ValidateConfigUpdate(config, proposed));
        proposed = config;
        proposed.MutableClusterStateDetails()->MutablePileRegistry()->SetRequiredFeatureLevel(
            NStorage::BridgeFeatureLevelPileOps);
        UNIT_ASSERT(NStorage::ValidateConfig(proposed));
    }

    // the list of piles can't be changed, piles can't be added or removed unless every node supports these bridge
    // features, dynamic nodes too, as nodes of older versions would be disconnected afterwards
    Y_UNIT_TEST(PileOperationsRequireFeatureLevel) {
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        config.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        FillTwoPileBridgedMetadata(state, config);
        TTestActorSystem runtime(6); // nodes 5 and 6 are dynamic ones
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        for (ui32 nodeId : {1u, 4u}) { // nodes of the pile staying in the cluster
            keepers.push_back(RegisterKeeper(runtime, config, nodeId, state, {}, &bridge));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }));

        std::vector<NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush> olderPushes, pushes;
        SubscribeDynamicNode(runtime, keepers.front(), 5, NStorage::BridgeFeatureLevelPileList, olderPushes);
        SubscribeDynamicNode(runtime, keepers.front(), 6, NStorage::BridgeFeatureLevel, pushes);

        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableRemoveBridgePile()->SetPileName("pile1");
        const TActorId edge = runtime.AllocateEdgeActor(1);
        runtime.Send(new IEventHandle(keepers.front(), edge, request.release()), 1);
        auto response = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_EQUAL_C(record.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            record.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(record.GetErrorReason(), "they have to be upgraded or stopped first NodeIds# 5");
        UNIT_ASSERT_C(!record.GetErrorReason().Contains("5 6"), record.GetErrorReason()); // the node of this version
        UNIT_ASSERT(!QueryKeeper(runtime, keepers.front()).GetQueryConfig().GetConfig().GetClusterStateDetails()
            .HasPendingPileOperation());
    }

    // the cluster can't be converted into bridged one unless every node supports the bridge features the converted
    // cluster requires, dynamic nodes too, as nodes of older versions abort or hang on inline piles
    Y_UNIT_TEST(ConversionRequiresFeatureLevel) {
        auto config = MakeStorageConfig(4);
        config.MutableSelfManagementConfig()->SetEnabled(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        TTestActorSystem runtime(6); // nodes 5 and 6 are dynamic ones
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        for (ui32 nodeId = 1; nodeId <= 4; ++nodeId) {
            keepers.push_back(RegisterKeeper(runtime, config, nodeId, state, {}, &bridge));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, keepers) == 1; }));

        std::vector<NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush> olderPushes, pushes;
        SubscribeDynamicNode(runtime, keepers.front(), 5, NStorage::BridgeFeatureLevelPileList, olderPushes);
        SubscribeDynamicNode(runtime, keepers.front(), 6, NStorage::BridgeFeatureLevel, pushes);

        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableConvertToBridge();
        const TActorId edge = runtime.AllocateEdgeActor(1);
        runtime.Send(new IEventHandle(keepers.front(), edge, request.release()), 1);
        auto response = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_EQUAL_C(record.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            record.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(record.GetErrorReason(), "they have to be upgraded or stopped first NodeIds# 5");
        UNIT_ASSERT_C(!record.GetErrorReason().Contains("5 6"), record.GetErrorReason()); // the node of this version
        UNIT_ASSERT(!QueryKeeper(runtime, keepers.front()).GetQueryConfig().GetConfig().HasClusterState());
    }

    // replaced config gets pile registry stored; new piles are INACTIVE, only INACTIVE piles can be dropped; change of
    // the list of piles or their order is reported, as it requires every node to support it
    Y_UNIT_TEST(UpdatePileRegistry) {
        auto makeBridgeConfig = [](const TVector<TString>& names) {
            NKikimrConfig::TBridgeConfig res;
            for (const TString& name : names) {
                res.AddPiles()->SetName(name);
            }
            return res;
        };
        const auto local = makeBridgeConfig({"pile0", "pile1"});
        auto update = [&](const NKikimrConfig::TBridgeConfig& bridgeConfig, NKikimrBlobStorage::TStorageConfig *config,
                const NKikimrConfig::TBridgeConfig *localBridgeConfig, bool expectChanged,
                const NKikimrConfig::TBridgeConfig *currentBridgeConfig = nullptr) {
            bool changed = true;
            const auto error = NStorage::UpdatePileRegistry(bridgeConfig, config, localBridgeConfig,
                currentBridgeConfig, &changed);
            if (!error) {
                UNIT_ASSERT_VALUES_EQUAL(changed, expectChanged);
                NStorage::TDistributedConfigKeeper::UpdateFingerprint(config);
                if (const auto error = NStorage::ValidateConfig(*config)) {
                    UNIT_FAIL(*error);
                }
            }
            return error;
        };
        auto piles = [](const NKikimrBlobStorage::TStorageConfig& config) {
            TStringBuilder res;
            const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
            for (size_t i = 0; i < registry.PilesSize(); ++i) {
                const auto& pile = registry.GetPiles(i);
                const auto state = config.GetClusterState().GetPerPileState(i);
                res << (res.empty() ? "" : " ") << (pile.HasName() ? pile.GetName() : "-") << ':'
                    << NKikimrBridge::TClusterState::EPileState_Name(state);
            }
            return res;
        };

        // implied registry gets persisted
        const auto initial = MakeTwoPileBridgedConfigWithoutRegistry();
        auto config = initial;
        if (const auto error = update(local, &config, &local, false)) {
            UNIT_FAIL(*error);
        }
        const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
        UNIT_ASSERT_VALUES_EQUAL(piles(config), "pile0:SYNCHRONIZED pile1:SYNCHRONIZED");
        UNIT_ASSERT(!registry.GetImplicitPileId());
        UNIT_ASSERT(!NStorage::IsPileListChanged(config)); // nodes taking pile ids from positions can work with it

        // persisted registry is kept as is
        auto again = config;
        if (const auto error = update(local, &again, &local, false)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT_VALUES_EQUAL(again.GetClusterStateDetails().GetPileRegistry().DebugString(), registry.DebugString());

        for (const auto *base : {&initial, &std::as_const(config)}) {
            // piles keep their ids when reordered, new ones are INACTIVE
            for (const auto& [names, expected] : std::initializer_list<std::pair<TVector<TString>, TString>>{
                    {{"pile1", "pile0"}, "pile0:SYNCHRONIZED pile1:SYNCHRONIZED"},
                    {{"pile2", "pile1", "pile0"}, "pile0:SYNCHRONIZED pile1:SYNCHRONIZED pile2:INACTIVE"}}) {
                auto copy = *base;
                if (const auto error = update(makeBridgeConfig(names), &copy, &local, true)) {
                    UNIT_FAIL(*error);
                }
                UNIT_ASSERT_VALUES_EQUAL(piles(copy), expected);
                UNIT_ASSERT_VALUES_EQUAL(copy.GetClusterState().GetGeneration(),
                    base->GetClusterState().GetGeneration());
            }

            // piles being part of the cluster can't be dropped
            for (const TVector<TString>& names : {TVector<TString>{"pile0"}, TVector<TString>{"pile0", "pile2"},
                    TVector<TString>{}}) {
                auto copy = *base;
                const auto error = update(makeBridgeConfig(names), &copy, &local, true);
                UNIT_ASSERT(error);
                UNIT_ASSERT_STRING_CONTAINS(*error, names.empty()
                    ? "bridged cluster can't run without bridge config"
                    : "pile 'pile1' in state SYNCHRONIZED can't be removed from BridgeConfig");
            }
        }

        // INACTIVE piles can be dropped, their slots are reused
        auto added = config;
        if (const auto error = update(makeBridgeConfig({"pile0", "pile1", "pile2", "pile3"}), &added, &local, true)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT_VALUES_EQUAL(piles(added), "pile0:SYNCHRONIZED pile1:SYNCHRONIZED pile2:INACTIVE pile3:INACTIVE");
        if (const auto error = update(makeBridgeConfig({"pile0", "pile1", "pile3"}), &added, &local, true)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT_VALUES_EQUAL(piles(added), "pile0:SYNCHRONIZED pile1:SYNCHRONIZED -:INACTIVE pile3:INACTIVE");

        // the list of piles has been changed, nodes taking pile ids from positions can't work with the cluster anymore,
        // and the change is not reported any more while the list stays the same
        UNIT_ASSERT(NStorage::IsPileListChanged(added));
        const auto withHole = makeBridgeConfig({"pile0", "pile1", "pile3"});
        auto unchanged = added;
        if (const auto error = update(withHole, &unchanged, &local, false, &withHole)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT(NStorage::IsPileListChanged(unchanged));

        // the same list of piles in the same order is not a change, though positions of piles don't match their ids;
        // reordering them is
        for (const auto& [names, expectChanged] : std::initializer_list<std::pair<TVector<TString>, bool>>{
                {{"pile0", "pile1", "pile3"}, false}, {{"pile3", "pile0", "pile1"}, true}}) {
            auto copy = added;
            if (const auto error = update(makeBridgeConfig(names), &copy, &local, expectChanged, &withHole)) {
                UNIT_FAIL(*error);
            }
            UNIT_ASSERT_VALUES_EQUAL(piles(copy), piles(added));
        }
        if (const auto error = update(makeBridgeConfig({"pile4", "pile0", "pile1", "pile3"}), &added, &local, true)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT_VALUES_EQUAL(piles(added), "pile0:SYNCHRONIZED pile1:SYNCHRONIZED pile4:INACTIVE pile3:INACTIVE");
        if (const auto error = update(local, &added, &local, true)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT_VALUES_EQUAL(piles(added), "pile0:SYNCHRONIZED pile1:SYNCHRONIZED");

        // the list of piles can't be changed while BSC converts the cluster
        auto converting = config;
        converting.MutableClusterStateDetails()->SetBscConversionPending(true);
        const auto error = update(makeBridgeConfig({"pile0", "pile1", "pile2"}), &converting, &local, true);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "can't be changed until BSC finishes conversion");

        // the feature level required since pile operations stays when the list of piles changes and when it doesn't;
        // the latter is not reported as a change
        auto required = config;
        const ui32 pileOpsLevel = NStorage::BridgeFeatureLevelPileOps;
        required.MutableClusterStateDetails()->MutablePileRegistry()->SetRequiredFeatureLevel(pileOpsLevel);
        for (const auto& [names, expectChanged] : std::initializer_list<std::pair<TVector<TString>, bool>>{
                {{"pile0", "pile1"}, false}, {{"pile0", "pile1", "pile2"}, true}}) {
            if (const auto error = update(makeBridgeConfig(names), &required, &local, expectChanged)) {
                UNIT_FAIL(*error);
            }
            UNIT_ASSERT_VALUES_EQUAL(required.GetClusterStateDetails().GetPileRegistry().GetRequiredFeatureLevel(),
                pileOpsLevel);
            UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(required), pileOpsLevel);
        }

        // empty pile name is persisted as a name, not as a free slot
        const auto withEmpty = makeBridgeConfig({"", "pile1"});
        auto empty = MakeTwoPileBridgedConfigWithoutRegistry("", "pile1");
        if (const auto error = update(withEmpty, &empty, &withEmpty, false)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT(empty.GetClusterStateDetails().GetPileRegistry().GetPiles(0).HasName());
        UNIT_ASSERT_VALUES_EQUAL(empty.GetClusterStateDetails().GetPileRegistry().GetPiles(0).GetName(), "");

        // config of the cluster that is not bridged yet gets no registry
        auto plain = MakeStorageConfig(4);
        UNIT_ASSERT(!update(local, &plain, &local, false));
        UNIT_ASSERT(!plain.GetClusterStateDetails().HasPileRegistry());

        // node running without BridgeConfig can't replace config of bridged cluster
        auto copy = initial;
        UNIT_ASSERT(update(local, &copy, nullptr, false));
    }

    // INACTIVE piles have no state to sync, so they are not tracked in the history of unsynced cluster state changes
    Y_UNIT_TEST(UnsyncedHistoryIgnoresInactivePiles) {
        const auto prev = MakeTwoPileBridgedConfig("pile0", "pile1");
        NKikimrConfig::TBridgeConfig bridge;
        for (const char *name : {"pile0", "pile1", "pile2"}) {
            bridge.AddPiles()->SetName(name);
        }

        auto config = prev;
        bool changed = false;
        if (const auto error = NStorage::UpdatePileRegistry(bridge, &config, &bridge, nullptr, &changed)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT(changed);
        config.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        config.SetGeneration(prev.GetGeneration() + 1);
        config.MutablePrevConfig()->CopyFrom(prev);
        if (const auto error = NStorage::TDistributedConfigKeeper::TransformConfigBeforeCommit(&config)) {
            UNIT_FAIL(*error);
        }

        // the disconnected pile has to learn this change, while the INACTIVE one has nothing to learn
        const auto& cs = config.GetClusterState();
        UNIT_ASSERT_VALUES_EQUAL(cs.GetGeneration(), prev.GetClusterState().GetGeneration() + 1);
        const auto& history = config.GetClusterStateDetails().GetUnsyncedHistory();
        UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(history[0].UnsyncedPilesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(history[0].GetUnsyncedPiles(0), TBridgePileId::FromPileIndex(1).GetLocalDb());

        // INACTIVE piles (and the ones not listed anymore) are removed from history entries
        auto trimmed = config;
        auto *entry = trimmed.MutableClusterStateDetails()->MutableUnsyncedHistory(0);
        entry->ClearUnsyncedPiles();
        for (ui32 pileIndex : {2u, 3u}) {
            TBridgePileId::FromPileIndex(pileIndex).CopyToProto(entry,
                &NKikimrBridge::TClusterStateDetails::TUnsyncedHistory::AddUnsyncedPiles);
        }
        auto *next = trimmed.MutableClusterStateDetails()->AddUnsyncedHistory();
        next->MutableClusterState()->CopyFrom(cs);
        TBridgePileId::FromPileIndex(1).CopyToProto(next,
            &NKikimrBridge::TClusterStateDetails::TUnsyncedHistory::AddUnsyncedPiles);
        if (const auto error = NStorage::TDistributedConfigKeeper::TrimHistoryEntries(&trimmed)) {
            UNIT_FAIL(*error);
        }
        const auto& trimmedHistory = trimmed.GetClusterStateDetails().GetUnsyncedHistory();
        UNIT_ASSERT_VALUES_EQUAL(trimmedHistory.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(trimmedHistory[0].UnsyncedPilesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(trimmedHistory[1].UnsyncedPilesSize(), 1);
    }

    // node warden handing out the designated storage config and bridge info to the bridge connection checker
    class TStorageConfigProvider : public TActor<TStorageConfigProvider> {
        const std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> Config;
        const TBridgeInfo::TPtr BridgeInfo;

    public:
        TStorageConfigProvider(std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> config,
                TBridgeInfo::TPtr bridgeInfo)
            : TActor(&TThis::StateFunc)
            , Config(std::move(config))
            , BridgeInfo(std::move(bridgeInfo))
        {}

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNodeWardenQueryStorageConfig::EventType) {
                Send(ev->Sender, new TEvNodeWardenStorageConfig(Config, true, BridgeInfo, Config));
            }
        }
    };

    // nodes of the pile that is listed in BridgeConfig, but not added to the cluster, connect neither to other piles,
    // nor to each other; the ones trying to connect get the cluster config to learn that
    Y_UNIT_TEST(InactivePileNodesDontConnect) {
        // nodes 5 and 6 belong to the third pile, which is INACTIVE
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        for (ui32 nodeId : {5u, 6u}) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(nodeId);
            node->SetHost("node-" + std::to_string(nodeId));
            node->SetPort(19000 + nodeId);
            node->MutableLocation()->SetBridgePileName("pile2");
            node->MutableLocation()->SetDataCenter("dc0");
        }
        NKikimrConfig::TBridgeConfig bridge;
        for (const char *name : {"pile0", "pile1", "pile2"}) {
            bridge.AddPiles()->SetName(name);
        }
        bool changed = false;
        if (const auto error = NStorage::UpdatePileRegistry(bridge, &config, &bridge, nullptr, &changed)) {
            UNIT_FAIL(*error);
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        UNIT_ASSERT_EQUAL(config.GetClusterState().GetPerPileState(2), NKikimrBridge::TClusterState::INACTIVE);
        const auto configPtr = std::make_shared<const NKikimrBlobStorage::TStorageConfig>(config);

        TTestActorSystem runtime(6);
        runtime.GetAppDataBridgeConfig().CopyFrom(bridge);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };

        auto startChecker = [&](ui32 nodeId, ui32 pileIndex) {
            const auto bridgeInfo = NStorage::TDistributedConfigKeeper::GenerateBridgeInfo(config,
                *MakeNodeWardenConfig(config, &bridge), nodeId);
            runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), runtime.Register(
                new TStorageConfigProvider(configPtr, bridgeInfo), nodeId));
            runtime.RegisterService(GetNameserviceActorId(), runtime.Register(new TIgnoreActor, nodeId));
            return runtime.Register(CreateDistconfBridgeConnectionCheckerActor(TBridgePileId::FromPileIndex(pileIndex),
                bridge.GetPiles(pileIndex).GetName()), nodeId);
        };
        THashMap<TString, TString> outgoingParams;
        auto prepare = [&](TActorId checkerId, ui32 peerNodeId) -> std::optional<TString> {
            const TActorId edge = runtime.AllocateEdgeActor(checkerId.NodeId());
            runtime.Send(new IEventHandle(checkerId, edge,
                new TEvInterconnect::TEvPrepareOutgoingConnection(peerNodeId)), checkerId.NodeId());
            auto ev = runtime.WaitForEdgeActorEvent<TEvInterconnect::TEvPrepareOutgoingConnectionResult>(edge);
            if (const auto *error = std::get_if<TString>(&ev->Get()->Conclusion)) {
                return *error;
            }
            outgoingParams = std::get<THashMap<TString, TString>>(ev->Get()->Conclusion);
            return std::nullopt;
        };
        // peer of older version doesn't report it takes pile ids from pile registry
        auto check = [&](TActorId checkerId, ui32 peerNodeId, ui32 peerPileIndex,
                const NKikimrBlobStorage::TStorageConfig& peerConfig, bool olderPeer = false) {
            NKikimrBlobStorage::TConnectivityPayload payload;
            TBridgePileId::FromPileIndex(peerPileIndex).CopyToProto(&payload,
                &NKikimrBlobStorage::TConnectivityPayload::SetBridgePileId);
            payload.SetBridgePileName(bridge.GetPiles(peerPileIndex).GetName());
            payload.SetPileListChangeCapable(!olderPeer);
            payload.MutableStorageConfig()->CopyFrom(peerConfig);
            THashMap<TString, TString> params;
            UNIT_ASSERT(payload.SerializeToString(&params["distconf"]));
            const TActorId edge = runtime.AllocateEdgeActor(checkerId.NodeId());
            runtime.Send(new IEventHandle(checkerId, edge, new TEvInterconnect::TEvCheckIncomingConnection(peerNodeId,
                std::move(params))), checkerId.NodeId());
            auto ev = runtime.WaitForEdgeActorEvent<TEvInterconnect::TEvCheckIncomingConnectionResult>(edge);
            return std::make_pair(ev->Get()->ErrorReason, ev->Get()->ParamsToSend);
        };

        // node of the pile being part of the cluster connects within its pile and to other active piles only
        const TActorId active = startChecker(1, 0);
        UNIT_ASSERT(!prepare(active, 4));
        UNIT_ASSERT(!prepare(active, 2));
        const auto outgoing = prepare(active, 5);
        UNIT_ASSERT(outgoing);
        UNIT_ASSERT_STRING_CONTAINS(*outgoing, "disconnected pile");
        UNIT_ASSERT(!check(active, 4, 0, config).first);
        UNIT_ASSERT(!check(active, 2, 1, config).first);

        // the list of piles has been changed by adding the pile, so the configuration is sent with the key older
        // versions don't know, and the peers of older versions are refused without getting the configuration
        UNIT_ASSERT(NStorage::IsPileListChanged(config));
        UNIT_ASSERT(!prepare(active, 2));
        UNIT_ASSERT(!outgoingParams.contains("distconf"));
        UNIT_ASSERT(outgoingParams.contains("distconf-pile-list-changed"));
        auto older = config;
        older.SetGeneration(older.GetGeneration() - 1);
        const auto [olderError, olderParams] = check(active, 2, 1, older, true);
        UNIT_ASSERT(olderError);
        UNIT_ASSERT_STRING_CONTAINS(*olderError, "has to be upgraded");
        for (const auto& item : olderParams) {
            NKikimrBlobStorage::TConnectivityPayload reply;
            UNIT_ASSERT(reply.ParseFromString(item.second));
            UNIT_ASSERT(!reply.HasStorageConfig());
        }

        // freshly started node of INACTIVE pile is refused, but it gets the config of the cluster
        auto fresh = config;
        fresh.SetGeneration(0);
        fresh.MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(3));
        fresh.ClearClusterStateDetails(); // base config of the node has just the registry implied by BridgeConfig
        fresh.MutableClusterStateDetails()->MutablePileRegistry()->CopyFrom(
            config.GetClusterStateDetails().GetPileRegistry());
        const auto [error, params] = check(active, 5, 2, fresh);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "peer is not allowed to connect");
        UNIT_ASSERT(params.contains("distconf-pile-list-changed"));
        NKikimrBlobStorage::TConnectivityPayload reply;
        UNIT_ASSERT(reply.ParseFromString(params.at("distconf-pile-list-changed")));
        UNIT_ASSERT_EQUAL(reply.GetStorageConfig().GetClusterState().GetPerPileState(2),
            NKikimrBridge::TClusterState::INACTIVE);

        // knowing its pile is INACTIVE, node of this pile doesn't connect to the nodes of the same pile, though it
        // still tries other piles to get config updates
        const TActorId inactive = startChecker(5, 2);
        const auto samePile = prepare(inactive, 6);
        UNIT_ASSERT(samePile);
        UNIT_ASSERT_STRING_CONTAINS(*samePile, "disconnected pile");
        UNIT_ASSERT(!prepare(inactive, 1));
        const auto incoming = check(inactive, 6, 2, config).first;
        UNIT_ASSERT(incoming);
        UNIT_ASSERT_STRING_CONTAINS(*incoming, "can't connect to each other");
    }

    // the pile removed from the cluster and added again on the same (not formatted) hosts: its nodes have cluster state
    // older than the pile's addition, and the history of the cluster is trimmed since then, but they get the config of
    // the cluster, as the data of the previous incarnation of the pile was dropped; the node of the pile that went its
    // own way meanwhile (its config is newer) is refused, as well as the node of the pile never removed
    Y_UNIT_TEST(ReaddedPileOlderNodeGetsConfig) {
        const auto old = MakeTwoPileBridgedConfig("pile0", "pile1"); // nodes 1 and 4 are in pile0, 2 and 3 in pile1
        auto readded = old;
        const ui64 addedGeneration = old.GetClusterState().GetGeneration() + 5;
        readded.MutableClusterState()->SetGeneration(addedGeneration);
        readded.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        auto *details = readded.MutableClusterStateDetails();
        details->ClearUnsyncedHistory(); // trimmed when the pile was removed
        auto *pss = details->AddPileSyncState();
        TBridgePileId::FromPileIndex(1).CopyToProto(pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::SetBridgePileId);
        pss->SetBecameUnsyncedGeneration(addedGeneration);
        pss->SetUnsyncedBSC(true);
        auto *registry = details->MutablePileRegistry();
        registry->SetPileListChanged(true);
        registry->SetRequiredFeatureLevel(NStorage::BridgeFeatureLevel);
        readded.SetGeneration(old.GetGeneration() + 5);
        auto neverRemoved = readded;
        registry->MutablePiles(1)->SetAddedGeneration(addedGeneration);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&readded);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&neverRemoved);
        auto island = old; // the pile went on its own, its config got newer than the cluster's one
        island.SetGeneration(readded.GetGeneration() + 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&island);

        TTestActorSystem runtime(4);
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        runtime.Start();
        Y_DEFER { runtime.Stop(); };

        auto startChecker = [&](ui32 nodeId, ui32 pileIndex, const NKikimrBlobStorage::TStorageConfig& config) {
            const auto bridgeInfo = NStorage::TDistributedConfigKeeper::GenerateBridgeInfo(config,
                *MakeNodeWardenConfig(config, &bridge), nodeId);
            runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), runtime.Register(new TStorageConfigProvider(
                std::make_shared<const NKikimrBlobStorage::TStorageConfig>(config), bridgeInfo), nodeId));
            runtime.RegisterService(GetNameserviceActorId(), runtime.Register(new TIgnoreActor, nodeId));
            return runtime.Register(CreateDistconfBridgeConnectionCheckerActor(TBridgePileId::FromPileIndex(pileIndex),
                bridge.GetPiles(pileIndex).GetName()), nodeId);
        };
        // returns the error and the config the peer gets, if any
        auto check = [&](TActorId checkerId, ui32 peerNodeId, ui32 peerPileIndex,
                const NKikimrBlobStorage::TStorageConfig& peerConfig) {
            NKikimrBlobStorage::TConnectivityPayload payload;
            TBridgePileId::FromPileIndex(peerPileIndex).CopyToProto(&payload,
                &NKikimrBlobStorage::TConnectivityPayload::SetBridgePileId);
            payload.SetBridgePileName(bridge.GetPiles(peerPileIndex).GetName());
            payload.SetPileListChangeCapable(true);
            payload.SetBridgeFeatureLevel(NStorage::BridgeFeatureLevel);
            payload.MutableStorageConfig()->CopyFrom(peerConfig);
            THashMap<TString, TString> params;
            UNIT_ASSERT(payload.SerializeToString(&params["distconf"]));
            const TActorId edge = runtime.AllocateEdgeActor(checkerId.NodeId());
            runtime.Send(new IEventHandle(checkerId, edge, new TEvInterconnect::TEvCheckIncomingConnection(peerNodeId,
                std::move(params))), checkerId.NodeId());
            auto ev = runtime.WaitForEdgeActorEvent<TEvInterconnect::TEvCheckIncomingConnectionResult>(edge);
            std::optional<ui64> sentGeneration;
            for (const auto& [key, value] : ev->Get()->ParamsToSend) {
                NKikimrBlobStorage::TConnectivityPayload reply;
                UNIT_ASSERT(reply.ParseFromString(value));
                if (reply.HasStorageConfig()) {
                    sentGeneration = reply.GetStorageConfig().GetGeneration();
                }
            }
            return std::make_pair(ev->Get()->ErrorReason.value_or(""), sentGeneration);
        };

        // the node of the cluster gives its config to the node of the previous incarnation of the pile
        const TActorId cluster = startChecker(1, 0, readded);
        const auto [error, sentGeneration] = check(cluster, 2, 1, old);
        UNIT_ASSERT_C(!error.Contains("diverged"), error);
        UNIT_ASSERT(sentGeneration);
        UNIT_ASSERT_VALUES_EQUAL(*sentGeneration, readded.GetGeneration());

        // the node of the previous incarnation accepts the cluster's node too, it takes the config then
        const TActorId stale = startChecker(2, 1, old);
        const TString staleError = check(stale, 1, 0, readded).first;
        UNIT_ASSERT_C(!staleError.Contains("diverged"), staleError);

        // the island and the pile never removed are refused
        UNIT_ASSERT_STRING_CONTAINS(check(cluster, 3, 1, island).first, "diverged");
        const TActorId notReadded = startChecker(4, 0, neverRemoved);
        UNIT_ASSERT_STRING_CONTAINS(check(notReadded, 3, 1, old).first, "diverged");
    }

    // sessions with the peers of older versions established before the config has started requiring the bridge features
    // they don't support (changed list of piles, removed pile) are not checked again, so they get disconnected then
    Y_UNIT_TEST(OlderPeerSessionsDisconnected) {
        const auto config = MakeTwoPileBridgedConfig("pile0", "pile1"); // nodes 1 and 4 are in pile0, 2 and 3 in pile1
        UNIT_ASSERT(!NStorage::IsPileListChanged(config));
        auto changed = config;
        changed.MutableClusterStateDetails()->MutablePileRegistry()->SetPileListChanged(true);
        changed.SetGeneration(changed.GetGeneration() + 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&changed);
        auto removed = changed;
        removed.MutableClusterStateDetails()->MutablePileRegistry()->SetRequiredFeatureLevel(
            NStorage::BridgeFeatureLevelPileOps);
        removed.SetGeneration(removed.GetGeneration() + 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&removed);

        TTestActorSystem runtime(4);
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        THashSet<TActorId> disconnected;
        runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInterconnect::EvDisconnect) {
                disconnected.insert(ev->Recipient);
            }
            return true;
        };
        runtime.Start();
        Y_DEFER { runtime.Stop(); };

        const auto bridgeInfo = NStorage::TDistributedConfigKeeper::GenerateBridgeInfo(config,
            *MakeNodeWardenConfig(config, &bridge), 1);
        runtime.RegisterService(MakeBlobStorageNodeWardenID(1), runtime.Register(new TStorageConfigProvider(
            std::make_shared<const NKikimrBlobStorage::TStorageConfig>(config), bridgeInfo), 1));
        runtime.RegisterService(GetNameserviceActorId(), runtime.Register(new TIgnoreActor, 1));
        const TActorId checkerId = runtime.Register(CreateDistconfBridgeConnectionCheckerActor(
            TBridgePileId::FromPileIndex(0), "pile0"), 1);
        const TActorId edge = runtime.AllocateEdgeActor(1);

        // bridge feature level 0 is reported by the versions taking pile ids from positions in BridgeConfig, level 1 by the
        // ones reporting only PileListChangeCapable
        auto makeParams = [&](ui32 pileIndex, ui32 level) {
            NKikimrBlobStorage::TConnectivityPayload payload;
            TBridgePileId::FromPileIndex(pileIndex).CopyToProto(&payload,
                &NKikimrBlobStorage::TConnectivityPayload::SetBridgePileId);
            payload.SetBridgePileName(bridge.GetPiles(pileIndex).GetName());
            payload.SetPileListChangeCapable(level >= NStorage::BridgeFeatureLevelPileList);
            if (level > NStorage::BridgeFeatureLevelPileList) {
                payload.SetBridgeFeatureLevel(level);
            }
            payload.MutableStorageConfig()->CopyFrom(config);
            THashMap<TString, TString> params;
            UNIT_ASSERT(payload.SerializeToString(&params["distconf"]));
            return params;
        };
        auto incoming = [&](ui32 peerNodeId, ui32 pileIndex, ui32 level) {
            runtime.Send(new IEventHandle(checkerId, edge, new TEvInterconnect::TEvCheckIncomingConnection(peerNodeId,
                makeParams(pileIndex, level))), 1);
            auto ev = runtime.WaitForEdgeActorEvent<TEvInterconnect::TEvCheckIncomingConnectionResult>(edge, false);
            UNIT_ASSERT_C(!ev->Get()->ErrorReason, *ev->Get()->ErrorReason);
        };
        auto outgoing = [&](ui32 peerNodeId, ui32 pileIndex, ui32 level) {
            runtime.Send(new IEventHandle(checkerId, edge, new TEvInterconnect::TEvNotifyOutgoingConnectionEstablished(
                peerNodeId, true, makeParams(pileIndex, level))), 1);
            runtime.Sim([&] { return runtime.HasImmediateEvents(); });
        };
        auto applyConfig = [&](const NKikimrBlobStorage::TStorageConfig& config, size_t numDisconnected) {
            runtime.Send(new IEventHandle(checkerId, edge, new TEvNodeWardenStorageConfig(
                std::make_shared<const NKikimrBlobStorage::TStorageConfig>(config), true, bridgeInfo)), 1);
            runtime.Sim([&] { return disconnected.size() < numDisconnected || runtime.HasImmediateEvents(); });
        };
        auto proxies = [&](std::initializer_list<ui32> nodeIds) {
            THashSet<TActorId> res;
            runtime.WrapInActorContext(edge, [&] {
                for (ui32 nodeId : nodeIds) {
                    res.insert(TActivationContext::InterconnectProxy(nodeId));
                }
            });
            return res;
        };

        // older peers connect while the list of piles is the same
        incoming(2, 1, 0);
        incoming(4, 0, NStorage::BridgeFeatureLevelPileList);
        outgoing(3, 1, 0);
        UNIT_ASSERT(disconnected.empty());

        // the list gets changed, sessions of the peers taking pile ids from positions are dropped
        applyConfig(changed, 2);
        UNIT_ASSERT_EQUAL(disconnected, proxies({2, 3}));

        // a pile gets removed, sessions of the peers supporting only changes of the list of piles are dropped too
        disconnected.clear();
        applyConfig(removed, 1);
        UNIT_ASSERT_EQUAL(disconnected, proxies({4}));

        // outgoing session with older peer gets dropped as soon as it is established (incoming one is refused)
        disconnected.clear();
        outgoing(4, 0, NStorage::BridgeFeatureLevel);
        outgoing(3, 1, NStorage::BridgeFeatureLevelPileList);
        UNIT_ASSERT_EQUAL(disconnected, proxies({3}));
    }

    // nameservice replying with the node list built against older configuration (with fewer piles)
    class TStaleNameserviceStub : public TActor<TStaleNameserviceStub> {
    public:
        TStaleNameserviceStub()
            : TActor(&TThis::StateFunc)
        {}

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvInterconnect::EvListNodes) {
                return;
            }
            auto location = [](const char *pileName) {
                NActorsInterconnect::TNodeLocation res;
                res.SetBridgePileName(pileName);
                res.SetDataCenter("dc0");
                return TNodeLocation(res);
            };
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(1, "", "node-1", "node-1", 19001, location("pile0"), true);
            nodes->emplace_back(7, "", "node-7", "node-7", 19007, location("pile2"), false);
            nodes->emplace_back(8, "", "node-8", "node-8", 19008, location("pile0"), false);
            auto pileMap = std::make_shared<TEvInterconnect::TEvNodesInfo::TPileMap>(2); // no third pile there
            pileMap->at(0) = {1, 8};
            Send(ev->Sender, new TEvInterconnect::TEvNodesInfo(nodes, std::move(pileMap)));
        }
    };

    // connection checker disconnects dynamic nodes of the piles not allowing connections resolving their piles against
    // its own configuration, which may have more piles than the one of the nameservice
    Y_UNIT_TEST(StaleNodesInfoAfterPileAdded) {
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        for (ui32 nodeId : {5u, 6u}) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(nodeId);
            node->SetHost("node-" + std::to_string(nodeId));
            node->SetPort(19000 + nodeId);
            node->MutableLocation()->SetBridgePileName("pile2");
            node->MutableLocation()->SetDataCenter("dc0");
        }
        NKikimrConfig::TBridgeConfig bridge;
        for (const char *name : {"pile0", "pile1", "pile2"}) {
            bridge.AddPiles()->SetName(name);
        }
        bool changed = false;
        if (const auto error = NStorage::UpdatePileRegistry(bridge, &config, &bridge, nullptr, &changed)) {
            UNIT_FAIL(*error);
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        const auto configPtr = std::make_shared<const NKikimrBlobStorage::TStorageConfig>(config);

        TTestActorSystem runtime(8);
        runtime.GetAppDataBridgeConfig().CopyFrom(bridge);
        THashSet<TActorId> disconnected;
        runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInterconnect::EvDisconnect) {
                disconnected.insert(ev->Recipient);
            }
            return true;
        };
        runtime.Start();
        Y_DEFER { runtime.Stop(); };

        auto dynamicNameserviceConfig = MakeIntrusive<TDynamicNameserviceConfig>();
        dynamicNameserviceConfig->MaxStaticNodeId = 6;
        dynamicNameserviceConfig->MinDynamicNodeId = 7;
        dynamicNameserviceConfig->MaxDynamicNodeId = 8;
        runtime.GetNode(1)->AppData->DynamicNameserviceConfig = dynamicNameserviceConfig;

        const auto bridgeInfo = NStorage::TDistributedConfigKeeper::GenerateBridgeInfo(config,
            *MakeNodeWardenConfig(config, &bridge), 1);
        runtime.RegisterService(MakeBlobStorageNodeWardenID(1), runtime.Register(
            new TStorageConfigProvider(configPtr, bridgeInfo), 1));
        runtime.RegisterService(GetNameserviceActorId(), runtime.Register(new TStaleNameserviceStub, 1));
        runtime.Register(CreateDistconfBridgeConnectionCheckerActor(TBridgePileId::FromPileIndex(0), "pile0"), 1);
        runtime.Sim([&] { return disconnected.size() < 3; });

        // static nodes of INACTIVE pile and the dynamic one
        THashSet<TActorId> expected;
        const TActorId edge = runtime.AllocateEdgeActor(1);
        runtime.WrapInActorContext(edge, [&] {
            for (ui32 nodeId : {5u, 6u, 7u}) {
                expected.insert(TActivationContext::InterconnectProxy(nodeId));
            }
        });
        UNIT_ASSERT_EQUAL(disconnected, expected);
    }

    // nodes take bridge config from the main config merged with the storage one, where the main config has precedence;
    // bridged cluster can't lose its bridge config
    Y_UNIT_TEST(MergedBridgeConfig) {
        auto withPiles = [](TStringBuf piles) {
            return NJson::ReadJsonFastTree(TStringBuilder() << R"({"config": {"bridge_config": {"piles": )" << piles
                << "}}}");
        };
        const auto ab = withPiles(R"([{"name": "a"}, {"name": "b"}])");
        const auto c = withPiles(R"([{"name": "c"}])");
        const auto none = NJson::ReadJsonFastTree(R"({"config": {"erasure": "none"}})");
        auto piles = [](const NJson::TJsonValue& main, const NJson::TJsonValue *storage) {
            const auto bridgeConfig = NStorage::GetMergedBridgeConfig(main, storage, false);
            TStringBuilder res;
            for (const auto& pile : bridgeConfig.GetPiles()) {
                res << (res.empty() ? "" : " ") << pile.GetName();
            }
            return res;
        };
        UNIT_ASSERT_VALUES_EQUAL(piles(ab, nullptr), "a b");
        UNIT_ASSERT_VALUES_EQUAL(piles(ab, &c), "a b");
        UNIT_ASSERT_VALUES_EQUAL(piles(none, &c), "c");
        UNIT_ASSERT_VALUES_EQUAL(piles(NJson::TJsonValue(), &c), "c"); // main config is not stored
        UNIT_ASSERT_VALUES_EQUAL(piles(none, &none), "");
        UNIT_ASSERT_VALUES_EQUAL(piles(none, nullptr), "");

        const auto unknown = withPiles(R"([{"name": "a", "x": 1}])");
        UNIT_ASSERT_EXCEPTION(NStorage::GetMergedBridgeConfig(unknown, nullptr, false), yexception);
        UNIT_ASSERT_VALUES_EQUAL(NStorage::GetMergedBridgeConfig(unknown, nullptr, true).PilesSize(), 1);

        NKikimrConfig::TBridgeConfig local;
        local.AddPiles()->SetName("pile0");
        local.AddPiles()->SetName("pile1");
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        bool changed = false;
        const auto error = NStorage::UpdatePileRegistry(NStorage::GetMergedBridgeConfig(none, nullptr, false), &config,
            &local, nullptr, &changed);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "bridged cluster can't run without bridge config");
    }

    // cluster bridged before pile registry was introduced gets the registry implied by BridgeConfig persisted
    void CheckImpliedPileRegistryPersisted(const TString& firstPileName, const TString& secondPileName) {
        const auto config = MakeTwoPileBridgedConfigWithoutRegistry(firstPileName, secondPileName);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        FillTwoPileBridgedMetadata(state, config);
        TTestActorSystem runtime(4);
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName(firstPileName);
        bridge.AddPiles()->SetName(secondPileName);

        THashMap<ui32, TBridgeInfo::TPtr> reported;
        TrackReportedBridgeInfo(runtime, reported);

        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keepers;
        for (ui32 nodeId = 1; nodeId <= 4; ++nodeId) {
            keepers.push_back(RegisterKeeper(runtime, config, nodeId, state, {}, &bridge));
        }
        // the registry is persisted without any external request
        const auto key = std::make_pair(1u, TString("/dev/disk1_1"));
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return state.Metadata[key].GetCommittedStorageConfig().GetClusterStateDetails().HasPileRegistry();
        }));
        const auto current = QueryKeeper(runtime, keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(state.Metadata[key].GetCommittedStorageConfig().GetGeneration(),
            current.GetGeneration());

        const auto& registry = current.GetClusterStateDetails().GetPileRegistry();
        UNIT_ASSERT_VALUES_EQUAL(registry.PilesSize(), 2);
        UNIT_ASSERT(registry.GetPiles(0).HasName());
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(0).GetName(), firstPileName);
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(1).GetName(), secondPileName);
        UNIT_ASSERT(!registry.GetImplicitPileId());
        UNIT_ASSERT_GT(current.GetGeneration(), config.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(current.GetClusterState().GetGeneration(), config.GetClusterState().GetGeneration());

        for (ui32 nodeId = 1; nodeId <= 4; ++nodeId) {
            UNIT_ASSERT(reported.contains(nodeId));
            const auto& info = *reported[nodeId];
            UNIT_ASSERT_VALUES_EQUAL(info.Piles.size(), 2);
            UNIT_ASSERT_EQUAL(info.GetPileForNode(1), &info.Piles[0]);
            UNIT_ASSERT_EQUAL(info.GetPileForNode(3), &info.Piles[1]);
            UNIT_ASSERT_EQUAL(info.ResolvePile(firstPileName), &info.Piles[0]);
            UNIT_ASSERT_VALUES_EQUAL(info.Piles[0].Name, firstPileName);
            UNIT_ASSERT(!info.Piles[0].IsFreeSlot);
        }
    }

    Y_UNIT_TEST(ImpliedPileRegistryPersisted) {
        CheckImpliedPileRegistryPersisted("pile0", "pile1");
    }

    // empty pile name is accepted by bridge config, so a cluster may have one; it must not be taken for a free slot
    Y_UNIT_TEST(ImpliedPileRegistryWithEmptyPileName) {
        CheckImpliedPileRegistryPersisted("", "pile1");
    }

    Y_UNIT_TEST(DelayedConnectionNotificationDoesNotBreakBridgeGather) {
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        FillTwoPileBridgedMetadata(state, config);
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

    // turn the only static group into the pile of its own, as it happens when converting cluster to bridged one
    void MakeInlinePile(NKikimrBlobStorage::TStorageConfig& config) {
        NBridge::MakeInlinePile(config.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(0));
        config.MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(1));
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
    }

    Y_UNIT_TEST(InlinePileStaticGroupValidation) {
        auto config = MakeStorageConfig(17, 1, {1, 2, 3, 4, 5, 6, 7, 8}, TBlobStorageGroupType::Erasure4Plus2Block);
        MakeInlinePile(config);
        if (const auto error = NStorage::ValidateConfig(config)) {
            UNIT_FAIL(*error);
        }

        // pile generation must follow the generation of the group itself
        auto *group = config.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(0);
        group->MutableBridgeGroupState()->MutablePile(0)->SetGroupGeneration(2);
        const auto error = NStorage::ValidateConfig(config);
        UNIT_ASSERT(error);
        UNIT_ASSERT_VALUES_EQUAL(*error, "incorrect referenced group generation");
    }

    Y_UNIT_TEST(InlinePileStaticGroupQuorum) {
        auto config = MakeStorageConfig(17, 1, {1, 2, 3, 4, 5, 6, 7, 8}, TBlobStorageGroupType::Erasure4Plus2Block);
        MakeInlinePile(config);
        UNIT_ASSERT(HasQuorum(config, {1, 2, 3, 4, 5, 6}));
        UNIT_ASSERT(!HasQuorum(config, {1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17}));
    }

    Y_UNIT_TEST(ConvertStorageConfigToBridge) {
        auto initial = MakeStorageConfig(17, 1, {1, 2, 3, 4, 5, 6, 7, 8}, TBlobStorageGroupType::Erasure4Plus2Block);
        auto *ring = initial.MutableStateStorageConfig()->MutableRing(); // legacy ring
        ring->SetNToSelect(1);
        ring->AddNode(1);
        auto *ringGroup = initial.MutableSchemeBoardConfig()->AddRingGroups();
        ringGroup->SetNToSelect(1);
        ringGroup->AddNode(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&initial);

        NKikimrConfig::TBridgeConfig bridgeConfig;
        bridgeConfig.AddPiles()->SetName("pile0");
        auto twoPiles = bridgeConfig;
        twoPiles.AddPiles()->SetName("pile1");
        auto copy = initial;
        UNIT_ASSERT(NStorage::ConvertStorageConfigToBridge(&copy, twoPiles));

        auto config = initial;
        if (const auto error = NStorage::ConvertStorageConfigToBridge(&config, bridgeConfig)) {
            UNIT_FAIL(*error);
        }
        config.SetGeneration(initial.GetGeneration() + 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        if (const auto error = NStorage::ValidateConfigUpdate(initial, config)) {
            UNIT_FAIL(*error);
        }

        const auto& cs = config.GetClusterState();
        UNIT_ASSERT_VALUES_EQUAL(cs.PerPileStateSize(), 1);
        UNIT_ASSERT_EQUAL(cs.GetPerPileState(0), NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT(config.GetClusterStateDetails().GetBscConversionPending());

        // pile registry is persisted, the only pile is the implicit one
        const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
        UNIT_ASSERT_VALUES_EQUAL(registry.PilesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(0).GetName(), "pile0");
        UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&registry, &NKikimrBridge::TPileRegistry::GetImplicitPileId),
            TBridgePileId::FromPileIndex(0));
        // nodes of older versions, which don't handle inline piles, neither get the config nor connect
        UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(config), NStorage::BridgeFeatureLevel);

        // static group becomes an inline pile with bumped generation
        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        UNIT_ASSERT_VALUES_EQUAL(ss.GroupsSize(), 1);
        const auto& group = ss.GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(group.GetGroupGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(group.GetBridgeProxyGroupId(), group.GetGroupID());
        UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId),
            TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_VALUES_EQUAL(group.GetBridgeGroupState().PileSize(), 1);
        const auto& pile = group.GetBridgeGroupState().GetPile(0);
        UNIT_ASSERT_VALUES_EQUAL(pile.GetGroupId(), group.GetGroupID());
        UNIT_ASSERT_VALUES_EQUAL(pile.GetGroupGeneration(), 2);
        UNIT_ASSERT_EQUAL(pile.GetStage(), NKikimrBridge::TGroupState::SYNCED);
        UNIT_ASSERT_VALUES_EQUAL(ss.VDisksSize(), 8);
        for (const auto& vdisk : ss.GetVDisks()) {
            UNIT_ASSERT_VALUES_EQUAL(vdisk.GetVDiskID().GetGroupGeneration(), 2);
        }

        // ring groups get bound to the only pile, legacy ring is turned into the ring group
        const auto& stateStorage = config.GetStateStorageConfig();
        UNIT_ASSERT(!stateStorage.HasRing());
        UNIT_ASSERT_VALUES_EQUAL(stateStorage.RingGroupsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stateStorage.GetRingGroups(0).GetNode(0), 1);
        for (const auto *pb : {&stateStorage, &config.GetSchemeBoardConfig()}) {
            UNIT_ASSERT_VALUES_EQUAL(pb->RingGroupsSize(), 1);
            const auto& item = pb->GetRingGroups(0);
            UNIT_ASSERT(item.HasBridgePileId());
            UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&item, &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId),
                TBridgePileId::FromPileIndex(0));
        }

        // conversion can't be done twice
        auto again = config;
        const auto error = NStorage::ConvertStorageConfigToBridge(&again, bridgeConfig);
        UNIT_ASSERT(error);
        UNIT_ASSERT_VALUES_EQUAL(*error, "cluster is already bridged one");

        // piles can't be added until BSC converts its groups
        config.MutableClusterState()->AddPerPileState(NKikimrBridge::TClusterState::INACTIVE);
        config.MutableClusterStateDetails()->MutablePileRegistry()->AddPiles()->SetName("pile1");
        UNIT_ASSERT(NStorage::ValidateConfig(config));
        config.MutableClusterStateDetails()->SetBscConversionPending(false);
        UNIT_ASSERT(!NStorage::ValidateConfig(config));
    }

    // conversion of storage pools may be skipped, then there is nothing for BSC to convert, and the cluster supports
    // both kinds of storage from the very beginning; static groups become inline piles anyway
    Y_UNIT_TEST(ConvertStorageConfigSkippingStoragePools) {
        auto initial = MakeStorageConfig(17, 1, {1, 2, 3, 4, 5, 6, 7, 8}, TBlobStorageGroupType::Erasure4Plus2Block);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&initial);

        NKikimrConfig::TBridgeConfig bridgeConfig;
        bridgeConfig.AddPiles()->SetName("pile0");
        auto copy = initial;
        auto error = NStorage::ConvertStorageConfigToBridge(&copy, bridgeConfig, true);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "requires bridge_config.enable_non_uniform_storage");

        bridgeConfig.SetEnableNonUniformStorage(true);
        copy = initial;
        error = NStorage::ConvertStorageConfigToBridge(&copy, bridgeConfig);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "only after BSC conversion completes");

        auto config = initial;
        error = NStorage::ConvertStorageConfigToBridge(&config, bridgeConfig, true);
        UNIT_ASSERT_C(!error, error.value_or(TString()));
        config.SetGeneration(initial.GetGeneration() + 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        error = NStorage::ValidateConfigUpdate(initial, config);
        UNIT_ASSERT_C(!error, error.value_or(TString()));

        const auto& details = config.GetClusterStateDetails();
        UNIT_ASSERT(!details.GetBscConversionPending());
        UNIT_ASSERT(details.GetNonUniformStorageEnabled());
        UNIT_ASSERT_GE(NStorage::GetRequiredBridgeFeatureLevel(config), NStorage::BridgeFeatureLevelNonUniformStorage);
        const auto& group = config.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(group.GetBridgeProxyGroupId(), group.GetGroupID());
        UNIT_ASSERT_EQUAL(group.GetBridgeGroupState().GetPile(0).GetStage(), NKikimrBridge::TGroupState::SYNCED);

        // the activation persists in the config replaced later, and piles may be added right away
        bool pileListChanged = false;
        error = NStorage::UpdatePileRegistry(bridgeConfig, &config, &bridgeConfig, &bridgeConfig, &pileListChanged);
        UNIT_ASSERT_C(!error, error.value_or(TString()));
        auto disabled = bridgeConfig;
        disabled.SetEnableNonUniformStorage(false);
        error = NStorage::UpdatePileRegistry(disabled, &config, &bridgeConfig, &bridgeConfig, &pileListChanged);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(*error, "can't be disabled");
        config.MutableClusterState()->AddPerPileState(NKikimrBridge::TClusterState::INACTIVE);
        config.MutableClusterStateDetails()->MutablePileRegistry()->AddPiles()->SetName("pile1");
        error = NStorage::ValidateConfig(config);
        UNIT_ASSERT_C(!error, error.value_or(TString()));
    }

    // removal of the pile drops parts of bridged static groups in the pile (their VDisks are destroyed by the nodes) and
    // its state storage replicas; the inline pile of the converted cluster makes its group the pure bridge proxy one
    Y_UNIT_TEST(RemoveBridgePileFromConfig) {
        const auto pile0 = TBridgePileId::FromPileIndex(0);
        const auto pile1 = TBridgePileId::FromPileIndex(1);
        auto vdiskStatuses = [](const NKikimrBlobStorage::TStorageConfig& config) {
            TStringBuilder res;
            for (const auto& vdisk : config.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                res << VDiskIDFromVDiskID(vdisk.GetVDiskID()) << ':'
                    << NKikimrBlobStorage::EEntityStatus_Name(vdisk.GetEntityStatus()) << ' ';
            }
            return res;
        };
        // the config is valid, and it is a valid update of the config before removal (if any)
        auto check = [](NKikimrBlobStorage::TStorageConfig config, const NKikimrBlobStorage::TStorageConfig *prev) {
            config.SetGeneration(config.GetGeneration() + 1);
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
            const auto error = prev ? NStorage::ValidateConfigUpdate(*prev, config) : NStorage::ValidateConfig(config);
            if (error) {
                UNIT_FAIL(*error);
            }
        };

        // native cluster: the pile group goes away
        auto config = MakeTwoPileBridgedConfig("pile0", "pile1");
        for (const auto pileId : {pile0, pile1}) {
            auto *ringGroup = config.MutableStateStorageConfig()->AddRingGroups();
            ringGroup->SetNToSelect(1);
            ringGroup->AddNode(pileId == pile0 ? 1 : 3);
            pileId.CopyToProto(ringGroup, &NKikimrConfig::TStateStorageConfig::TRing::SetBridgePileId);
        }
        auto *details = config.MutableClusterStateDetails();
        auto *pss = details->AddPileSyncState();
        pile1.CopyToProto(pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::SetBridgePileId);
        pss->SetUnsyncedBSC(true);
        auto *op = details->MutablePendingPileOperation();
        pile1.CopyToProto(op, &NKikimrBridge::TClusterStateDetails::TPendingPileOperation::SetBridgePileId);
        op->SetKind(NKikimrBridge::TClusterStateDetails::TPendingPileOperation::REMOVE);
        UNIT_ASSERT(NStorage::TDistributedConfigKeeper::RemoveBridgePileFromConfig(&config, pile1)); // SYNCHRONIZED
        config.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        auto prev = config;
        if (const auto error = NStorage::TDistributedConfigKeeper::RemoveBridgePileFromConfig(&config, pile1)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT_EQUAL(config.GetClusterState().GetPerPileState(1), NKikimrBridge::TClusterState::INACTIVE);
        UNIT_ASSERT(!config.GetClusterStateDetails().PileSyncStateSize());
        UNIT_ASSERT(!config.GetClusterStateDetails().HasPendingPileOperation());
        // nodes may know the removed group, so its id is not reused
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetNextStaticGroupId(), 3);
        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        UNIT_ASSERT_VALUES_EQUAL(ss.GroupsSize(), 2);
        const auto& proxy = ss.GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(proxy.GetGroupGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(proxy.GetBridgeGroupState().PileSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ss.GetGroups(1).GetGroupID(), 1);
        UNIT_ASSERT_VALUES_EQUAL(vdiskStatuses(config), "[1:1:0:0:0]:INITIAL [2:1:0:0:0]:DESTROY ");
        UNIT_ASSERT_VALUES_EQUAL(ss.PDisksSize(), 2); // the destroyed VDisk still occupies its slot
        UNIT_ASSERT_VALUES_EQUAL(config.GetStateStorageConfig().RingGroupsSize(), 1);
        UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&config.GetStateStorageConfig().GetRingGroups(0),
            &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId), pile0);
        check(config, &prev);
        // static parts may go away only along with the pile
        auto stay = config;
        stay.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        stay.SetGeneration(stay.GetGeneration() + 1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&stay);
        UNIT_ASSERT(NStorage::ValidateConfigUpdate(prev, stay));

        // converted cluster which has got the second pile: the inline pile goes away, its group stays as proxy
        config = MakeStorageConfig(4, 1, {1});
        for (auto& node : *config.MutableAllNodes()) {
            node.MutableLocation()->SetBridgePileName(node.GetNodeId() <= 2 ? "pile0" : "pile1");
        }
        NKikimrConfig::TBridgeConfig bridgeConfig;
        bridgeConfig.AddPiles()->SetName("pile0");
        if (const auto error = NStorage::ConvertStorageConfigToBridge(&config, bridgeConfig)) {
            UNIT_FAIL(*error);
        }
        details = config.MutableClusterStateDetails();
        details->ClearBscConversionPending();
        details->MutablePileRegistry()->AddPiles()->SetName("pile1");
        AddMultiVDiskGroup(config, {3}, TBlobStorageGroupType::ErasureNone, 1);
        auto *ssm = config.MutableBlobStorageConfig()->MutableServiceSet();
        ssm->MutableGroups(1)->SetBridgeProxyGroupId(0);
        pile1.CopyToProto(ssm->MutableGroups(1), &NKikimrBlobStorage::TGroupInfo::SetBridgePileId);
        auto *pile = NBridge::AddGroupPile(ssm->MutableGroups(0)->MutableBridgeGroupState(), pile1);
        pile->SetGroupId(1);
        pile->SetGroupGeneration(1);
        pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
        auto *clusterState = config.MutableClusterState();
        clusterState->AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        clusterState->SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        pile1.CopyToProto(clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        pile1.CopyToProto(clusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        check(config, nullptr);
        prev = config;
        if (const auto error = NStorage::TDistributedConfigKeeper::RemoveBridgePileFromConfig(&config, pile0)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT(!config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId());
        const auto& group = config.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(group.GetGroupGeneration(), 3);
        UNIT_ASSERT(!group.RingsSize());
        UNIT_ASSERT(!group.HasErasureSpecies());
        UNIT_ASSERT(!group.HasBridgePileId());
        UNIT_ASSERT(!group.HasBridgeProxyGroupId());
        UNIT_ASSERT_VALUES_EQUAL(group.GetBridgeGroupState().PileSize(), 2);
        UNIT_ASSERT(!NBridge::GetGroupPile(group.GetBridgeGroupState(), pile0));
        UNIT_ASSERT_VALUES_EQUAL(NBridge::GetGroupPile(group.GetBridgeGroupState(), pile1)->GetGroupId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(vdiskStatuses(config), "[0:2:0:0:0]:DESTROY [1:1:0:0:0]:INITIAL ");
        check(config, &prev);
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

    bool FailSubscriptionAcrossCut(TTestActorSystem& runtime, ui32 nodeId, const IEventHandle& ev,
                                   const THashMap<TActorId, ui32>& peerByProxy) {
        if (ev.GetTypeRewrite() != TEvents::TSystem::Subscribe) {
            return false;
        }
        const auto it = peerByProxy.find(ev.GetRecipientRewrite());
        if (it == peerByProxy.end()
            || !((nodeId == 1 && it->second == 2) || (nodeId == 2 && it->second == 1))) {
            return false;
        }
        ScheduleSubscriptionFailure(runtime, nodeId, ev, it->second, TDuration::MilliSeconds(10));
        return true;
    }

    void CheckRootAfterNodeLoss(bool loseStorageQuorum) {
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
                    ScheduleSubscriptionFailure(runtime, nodeId, *ev, it->second, TDuration::MilliSeconds(10));
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
        CheckRootAfterNodeLoss(false);
    }

    Y_UNIT_TEST(StorageQuorumLossStillReleasesRoot) {
        CheckRootAfterNodeLoss(true);
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
        RootDiskLost,
        RootDiskLostWithStalledRead,
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
        NKikimrBlobStorage::TStorageConfig originalConfig;
        const NKikimrBlobStorage::TPDiskMetadataRecord emptyMetadata;
        const auto metadata = [&](ui32 nodeId) -> const NKikimrBlobStorage::TPDiskMetadataRecord& {
            const auto it = state.Metadata.find(std::pair<ui32, TString>{
                nodeId, "/dev/disk" + std::to_string(nodeId) + "_1"});
            return it != state.Metadata.end() ? it->second : emptyMetadata;
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
            originalConfig.CopyFrom(committed);
            UNIT_ASSERT_VALUES_EQUAL(committed.GetGeneration(), 1u);
            UNIT_ASSERT(HasQuorum(committed, {1}));
            for (ui32 nodeId : {2, 3}) {
                UNIT_ASSERT(!metadata(nodeId).HasCommittedStorageConfig());
                UNIT_ASSERT_VALUES_EQUAL(metadata(nodeId).GetProposedStorageConfig().GetGeneration(), 1u);
            }
        }

        const bool rootDiskLost = order == ERestartOrder::RootDiskLost
            || order == ERestartOrder::RootDiskLostWithStalledRead;
        if (rootDiskLost) {
            state.Metadata.erase(std::pair<ui32, TString>{1, "/dev/disk1_1"});
        }
        state.RequiredRootNodeId = 0;
        if (rootDiskLost) {
            state.RequiredRootNodeId = 1;
        }
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
        bool stallSurvivorReads = order == ERestartOrder::RootDiskLostWithStalledRead;
        TActorId survivorStartupReader;
        ui32 expiredSurvivorReads = 0;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType) {
                if (!survivorStartupReader) {
                    survivorStartupReader = ev->Sender;
                } else if (stallSurvivorReads && ev->Sender != survivorStartupReader) {
                    ++expiredSurvivorReads;
                    return false;
                }
            }
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

        const TString fingerprint = originalConfig.GetFingerprint();
        const ui32 rootNodeId = *FindConvergedRoot(runtime, keeperIds);
        const auto root = std::ranges::find_if(keeperIds, [&](TActorId id) { return id.NodeId() == rootNodeId; });
        if (stallSurvivorReads) {
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, *root).GetQueryConfig().GetConfig().GetGeneration(), 0);
            const TActorId edge = runtime.AllocateEdgeActor(rootNodeId);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableBootstrapCluster()->SetSelfAssemblyUUID("partial-bootstrap-test");
            runtime.Send(new IEventHandle(*root, edge, request.release()), rootNodeId);
            const auto response = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
            UNIT_ASSERT_C(response->Get()->Record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                "partial bootstrap read acknowledged a newly generated configuration");
            UNIT_ASSERT(expiredSurvivorReads);
            stallSurvivorReads = false;
            UNIT_ASSERT_C(SimUntil(runtime, [&] {
                return std::ranges::all_of(keeperIds, [&](TActorId id) {
                    return metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() == fingerprint;
                });
            }), "proposed bootstrap quorum was not recovered after the read resumed");
        }
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

    Y_UNIT_TEST(BootstrapRecoversAfterRootDiskLoss) {
        CheckPartialBootstrapRestart(ERestartOrder::RootDiskLost);
    }

    Y_UNIT_TEST(BootstrapWaitsForSurvivorRead) {
        CheckPartialBootstrapRestart(ERestartOrder::RootDiskLostWithStalledRead);
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
        auto initial = MakeStorageConfig(9, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9});
        initial.MutableSelfManagementConfig()->SetEnabled(true);
        initial.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        auto *bs = initial.MutableBlobStorageConfig();
        auto *serviceSet = bs->MutableServiceSet();
        auto *group = serviceSet->MutableGroups(0);
        group->SetErasureSpecies(TBlobStorageGroupType::ErasureMirror3dc);
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
        UNIT_ASSERT(NStorage::HasConfigQuorum(newer, available, {}, *nwConfig, false));
        UNIT_ASSERT(NStorage::HasConfigQuorum(newer, newerDisks, {}, *nwConfig, false));

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

    Y_UNIT_TEST(CommittedConfigQuorumArrivesAfterCollection) {
        auto config = MakeStorageConfig(8, 1, {1, 2, 3, 4, 5, 6, 7, 8},
                                        TBlobStorageGroupType::Erasure4Plus2Block);
        config.MutableSelfManagementConfig()->SetEnabled(true);
        for (auto& node : *config.MutableAllNodes()) {
            node.MutableLocation()->SetDataCenter("dc-1");
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        auto updated = config;
        updated.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&updated);

        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        for (ui32 nodeId = 1; nodeId <= 8; ++nodeId) {
            auto& record = state.Metadata[std::pair<ui32, TString>{
                nodeId, "/dev/disk" + std::to_string(nodeId) + "_1"}];
            record.MutableCommittedStorageConfig()->CopyFrom(nodeId <= 6 ? updated : config);
        }

        TTestActorSystem runtime(8);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::vector<TActorId> keeperIds;
        // These six nodes have storage quorum, but only five have generation 2.
        for (ui32 nodeId : {1, 2, 3, 4, 5, 7}) {
            keeperIds.push_back(RegisterKeeper(runtime, config, nodeId, state));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return FindConvergedRoot(runtime, keeperIds) == 1 && state.SawScatter;
        }));
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));

        for (ui32 nodeId : {6, 8}) {
            keeperIds.push_back(RegisterKeeper(runtime, config, nodeId, state));
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(state.Metadata, [&](const auto& item) {
                return item.second.GetCommittedStorageConfig().GetFingerprint() == updated.GetFingerprint();
            });
        }), "committed configuration was not collected again after its quorum joined");
    }

    enum class ECommitConfirmationScenario {
        DelayedCommit,
        Timeout,
        StalledRead,
        ConflictingCommit,
        NewerCommit,
        RootChange,
        DryRun,
    };

    struct TCommitConfirmationFixture {
        NKikimrBlobStorage::TStorageConfig Config;
        TNodeWardenTestState State;
        TTestActorSystem Runtime;
        std::vector<TActorId> Keepers;

        explicit TCommitConfirmationFixture(bool nodeMajority = false, ui32 runtimeNodeCount = 3)
            : Config(MakeStorageConfig(3, 1, nodeMajority ? std::vector<ui32>{} : std::vector<ui32>{1}))
            , Runtime(runtimeNodeCount)
        {
            Config.MutableSelfManagementConfig()->SetEnabled(true);
            for (auto& node : *Config.MutableAllNodes()) {
                node.MutableLocation()->SetDataCenter("dc-1");
            }
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&Config);
            State.RequiredRootNodeId = nodeMajority ? 0 : 1;
            for (ui32 nodeId : {1, 2, 3}) {
                Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(Config);
            }
        }

        ~TCommitConfirmationFixture() {
            Runtime.Stop();
        }

        NKikimrBlobStorage::TPDiskMetadataRecord& Metadata(ui32 nodeId) {
            return State.Metadata[std::pair<ui32, TString>{nodeId, "/dev/disk" + std::to_string(nodeId) + "_1"}];
        }

        void UseMirror3dcGroup(ui32 nodeCount = 3) {
            Config = MakeStorageConfig(nodeCount, 1, {1, 2, 3}, TBlobStorageGroupType::ErasureMirror3dc);
            Config.MutableSelfManagementConfig()->SetEnabled(true);
            Config.MutableSelfManagementConfig()->SetErasureSpecies("mirror-3-dc");
            for (auto& node : *Config.MutableAllNodes()) {
                node.MutableLocation()->SetDataCenter("dc-" + std::to_string((node.GetNodeId() - 1) % 3 + 1));
            }
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&Config);
            for (const auto& node : Config.GetAllNodes()) {
                Metadata(node.GetNodeId()).MutableCommittedStorageConfig()->CopyFrom(Config);
            }
        }

        TString MakeReplacementYaml(const NKikimrBlobStorage::TStorageConfig& config, ui64 version,
                                    TStringBuf erasure) const {
            TStringBuilder yaml;
            yaml << "metadata: {kind: MainConfig, version: " << version << ", cluster: ''}\n"
                 << "config:\n"
                 << "  erasure: " << erasure << '\n'
                 << "  self_management_config: {enabled: true, erasure_species: " << erasure << "}\n"
                 << "  default_disk_type: ROT\n"
                 << "  hosts:\n";
            for (const auto& node : config.GetAllNodes()) {
                const auto& hosts = config.GetBlobStorageConfig().GetDefineBox().GetHost();
                const auto host = std::ranges::find_if(hosts, [&](const auto& item) {
                    return item.GetEnforcedNodeId() == node.GetNodeId();
                });
                UNIT_ASSERT(host != hosts.end());
                const auto& hostConfigs = config.GetBlobStorageConfig().GetDefineHostConfig();
                const auto hostConfig = std::ranges::find_if(hostConfigs, [&](const auto& item) {
                    return item.GetHostConfigId() == host->GetHostConfigId();
                });
                UNIT_ASSERT(hostConfig != hostConfigs.end());
                yaml << "  - node_id: " << node.GetNodeId() << '\n'
                     << "    host: " << node.GetHost() << '\n'
                     << "    port: " << node.GetPort() << '\n'
                     << "    location: {data_center: " << node.GetLocation().GetDataCenter();
                if (node.GetLocation().HasBridgePileName()) {
                    yaml << ", bridge_pile_name: " << node.GetLocation().GetBridgePileName();
                }
                yaml << "}\n"
                     << "    drive:\n";
                for (const auto& drive : hostConfig->GetDrive()) {
                    yaml << "    - {path: " << drive.GetPath()
                         << ", type: " << NKikimrBlobStorage::EPDiskType_Name(drive.GetType()) << "}\n";
                }
            }
            return yaml;
        }

        TString MakeReplacementYaml(ui64 version = 0, TStringBuf erasure = "none") const {
            return MakeReplacementYaml(Config, version, erasure);
        }

        auto MakeReplaceRequest(ui64 version = 0, TStringBuf erasure = "none", const TString& suffix = {}) const {
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableReplaceStorageConfig()->SetYAML(MakeReplacementYaml(version, erasure) + suffix);
            request->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(true);
            return request;
        }

        TActorId Root() {
            const auto rootNodeId = FindConvergedRoot(Runtime, Keepers);
            UNIT_ASSERT(rootNodeId);
            return Keepers[*rootNodeId - 1];
        }

        THashMap<TActorId, ui32> InterconnectPeers() {
            THashMap<TActorId, ui32> result;
            for (const TActorId keeper : Keepers) {
                UNIT_ASSERT(Runtime.WrapInActorContext(keeper, [&](IActor*) {
                    for (ui32 peer : {1, 2, 3}) {
                        if (peer != keeper.NodeId()) {
                            result.emplace(TActivationContext::InterconnectProxy(peer), peer);
                        }
                    }
                }));
            }
            return result;
        }

        void Disconnect(ui32 nodeId, ui32 peerNodeId) {
            UNIT_ASSERT(Runtime.WrapInActorContext(Keepers[nodeId - 1], [&](IActor*) {
                TActivationContext::Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0,
                                                          TActivationContext::InterconnectProxy(peerNodeId), {}, nullptr, 0));
            }));
        }

        void Start(bool initialBase = false, const NKikimrConfig::TBridgeConfig *bridgeConfig = nullptr) {
            auto baseConfig = Config;
            if (initialBase) {
                baseConfig.SetGeneration(0);
                NStorage::TDistributedConfigKeeper::UpdateFingerprint(&baseConfig);
            }
            Runtime.Start();
            for (const auto& node : Config.GetAllNodes()) {
                Keepers.push_back(RegisterKeeper(Runtime, baseConfig, node.GetNodeId(), State, {}, bridgeConfig));
            }
            UNIT_ASSERT(SimUntil(Runtime, [&] { return FindConvergedRoot(Runtime, Keepers).has_value(); }));
            UNIT_ASSERT(QueryKeeper(Runtime, Root()).HasScepter());
        }

        void StartWithBoundChild(ui32 rootNodeId, ui32 childNodeId) {
            Runtime.Start();
            Keepers.resize(Config.AllNodesSize());
            for (ui32 id : {rootNodeId, childNodeId}) {
                Keepers[id - 1] = RegisterKeeper(Runtime, Config, id, State);
            }
            UNIT_ASSERT(SimUntil(Runtime, [&] {
                ui32 root = 0;
                UNIT_ASSERT(Runtime.WrapInActorContext(Keepers[childNodeId - 1], [&](IActor *actor) {
                    root = static_cast<NStorage::TDistributedConfigKeeper*>(actor)->GetRootNodeId();
                }));
                return root == rootNodeId;
            }, TDuration::Seconds(90)));
            for (const auto& node : Config.GetAllNodes()) {
                if (!Keepers[node.GetNodeId() - 1]) {
                    Keepers[node.GetNodeId() - 1] = RegisterKeeper(Runtime, Config, node.GetNodeId(), State);
                }
            }
            UNIT_ASSERT(SimUntil(Runtime, [&] { return FindConvergedRoot(Runtime, Keepers) == rootNodeId; }));
            UNIT_ASSERT(QueryKeeper(Runtime, Root()).HasScepter());
        }

        void FormatKeeper(ui32 nodeId) {
            Runtime.StopNode(nodeId);
            Metadata(nodeId).Clear();
            Runtime.StartNode(nodeId);
            auto base = Config;
            base.SetGeneration(0);
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&base);
            Keepers[nodeId - 1] = RegisterKeeper(Runtime, base, nodeId, State);
        }
    };

    using TCollectedConfigs = NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs;

    TCollectedConfigs MakeCollectedConfigs(const NKikimrBlobStorage::TStorageConfig& config,
                                           std::initializer_list<ui32> nodeIds = {1, 2, 3}) {
        TCollectedConfigs collected;
        auto *nodes = collected.AddNodes();
        nodes->MutableBaseConfig()->CopyFrom(config);
        nodes->MutableBaseConfig()->SetGeneration(0);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(nodes->MutableBaseConfig());
        for (const ui32 nodeId : nodeIds) {
            nodes->AddNodeIds()->CopyFrom(config.GetAllNodes(nodeId - 1));
        }
        return collected;
    }

    void AddCollectedConfig(TCollectedConfigs& collected, const NKikimrBlobStorage::TStorageConfig& config,
                            std::initializer_list<ui32> nodeIds, bool committed = true) {
        auto *item = committed ? collected.AddCommittedConfigs() : collected.AddProposedConfigs();
        item->MutableConfig()->CopyFrom(config);
        for (const ui32 nodeId : nodeIds) {
            auto *disk = item->AddDisks();
            disk->MutableNodeId()->CopyFrom(config.GetAllNodes(nodeId - 1));
            disk->SetPath("/dev/disk" + std::to_string(nodeId) + "_1");
        }
    }

    NKikimrBlobStorage::TStorageConfig MakeNextConfig(const NKikimrBlobStorage::TStorageConfig& config,
                                                     ui64 generation = 2) {
        auto updated = config;
        updated.SetGeneration(generation);
        updated.MutablePrevConfig()->CopyFrom(config);
        updated.MutablePrevConfig()->ClearPrevConfig();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&updated);
        return updated;
    }

    NStorage::TDistributedConfigKeeper::TProcessCollectConfigsResult ProcessCollectedConfigs(
            TCommitConfirmationFixture& fixture, TCollectedConfigs& collected, bool dryRun = false) {
        NStorage::TDistributedConfigKeeper::TProcessCollectConfigsResult result;
        UNIT_ASSERT(fixture.Runtime.WrapInActorContext(fixture.Root(), [&](IActor *actor) {
            result = static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ProcessCollectConfigs(
                &collected, std::nullopt, dryRun);
        }));
        return result;
    }

    void CheckDurableConfigConverges(TCommitConfirmationFixture& fixture,
                                     const NKikimrBlobStorage::TStorageConfig& config) {
        const auto matches = [&] {
            return std::ranges::all_of(fixture.Keepers, [&](TActorId id) {
                return fixture.Metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint()
                       == config.GetFingerprint();
            });
        };
        const bool converged = SimUntil(fixture.Runtime, matches);
        TStringBuilder state;
        if (!converged) {
            for (const TActorId id : fixture.Keepers) {
                const auto& metadata = fixture.Metadata(id.NodeId());
                state << " node=" << id.NodeId() << " committed=" << metadata.GetCommittedStorageConfig().GetGeneration()
                      << " proposed=" << metadata.GetProposedStorageConfig().GetGeneration()
                      << " matches=" << (metadata.GetCommittedStorageConfig().GetFingerprint() == config.GetFingerprint());
                fixture.Runtime.WrapInActorContext(id, [&](IActor *actor) {
                    const auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
                    state << " root=" << keeper->GetRootNodeId() << " nodeQuorum=" << keeper->PartOfNodeQuorum();
                });
            }
        }
        UNIT_ASSERT_C(converged, "configuration did not reach every disk:" << state);
        UNIT_ASSERT_C(!SimUntil(fixture.Runtime, [&] { return !matches(); }, TDuration::Seconds(3)),
                      "a delayed operation changed the converged configuration");
    }

    Y_UNIT_TEST(RecoveryUsesLatestCommitted) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto updated = MakeNextConfig(fixture.Config);
        auto collected = MakeCollectedConfigs(fixture.Config, {1, 3});
        AddCollectedConfig(collected, updated, {1});
        AddCollectedConfig(collected, fixture.Config, {3});

        const auto result = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetFingerprint(),
                                 updated.GetFingerprint());
        CheckDurableConfigConverges(fixture, updated);
    }

    Y_UNIT_TEST(RecoveryExcludesEmptyDisks) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto updated = MakeNextConfig(fixture.Config);
        auto collected = MakeCollectedConfigs(fixture.Config, {1, 2});
        AddCollectedConfig(collected, updated, {1});
        auto *emptyDisk = collected.AddNoMetadata();
        emptyDisk->MutableNodeId()->CopyFrom(fixture.Config.GetAllNodes(1));
        emptyDisk->SetPath("/dev/disk2_1");

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(result.ErrorReason, "an empty disk formed a nonzero recovery quorum");
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 1);
    }

    void TestRecoveryRetainsPublishedProposal(const NKikimrConfig::TBridgeConfig *bridgeConfig) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true, bridgeConfig);
        const TActorId root = fixture.Root();
        const ui32 sourceNodeId = root.NodeId() % 3 + 1;
        const auto updated = MakeNextConfig(fixture.Config);
        UNIT_ASSERT(fixture.Runtime.WrapInActorContext(root, [&](IActor *actor) {
            auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
            const auto& source = fixture.Config.GetAllNodes(sourceNodeId - 1);
            keeper->UpdateBound(sourceNodeId, source, updated, nullptr);
            keeper->UpdateBound(sourceNodeId, source, fixture.Config, nullptr);
        }));
        auto collected = MakeCollectedConfigs(fixture.Config);
        AddCollectedConfig(collected, fixture.Config, {1, 2, 3});
        AddCollectedConfig(collected, updated, {sourceNodeId}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetFingerprint(),
                                 updated.GetFingerprint());
        CheckDurableConfigConverges(fixture, updated);
    }

    Y_UNIT_TEST(RecoveryRetainsPublishedProposal) {
        TestRecoveryRetainsPublishedProposal(nullptr);
    }

    // nodes of the cluster prepared for conversion to bridged one run with single-pile BridgeConfig, while the cluster
    // is still the ordinary one for configuration management, and its configuration is recovered the same way
    Y_UNIT_TEST(RecoveryRetainsPublishedProposalBeforeConversion) {
        NKikimrConfig::TBridgeConfig bridgeConfig;
        bridgeConfig.AddPiles()->SetName("pile_1");
        TestRecoveryRetainsPublishedProposal(&bridgeConfig);
    }

    Y_UNIT_TEST(RecoveryIgnoresUnpublishedMinorityProposal) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        auto collected = MakeCollectedConfigs(fixture.Config);
        AddCollectedConfig(collected, fixture.Config, {1, 2, 3});
        AddCollectedConfig(collected, MakeNextConfig(fixture.Config), {2}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT(QueryKeeper(fixture.Runtime, fixture.Root()).HasScepter());
    }

    Y_UNIT_TEST(RecoveryReproposesPossiblyChosenProposal) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        auto updated = MakeNextConfig(fixture.Config);
        updated.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&updated);
        auto collected = MakeCollectedConfigs(fixture.Config, {2, 3});
        AddCollectedConfig(collected, fixture.Config, {2, 3});
        AddCollectedConfig(collected, updated, {2}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        UNIT_ASSERT(result.RequiresRecoveryProposition);
        UNIT_ASSERT(result.ConfigToPropose);
        UNIT_ASSERT(result.PropositionBase);
        UNIT_ASSERT_VALUES_EQUAL(result.ConfigToPropose->GetFingerprint(), updated.GetFingerprint());
        UNIT_ASSERT_VALUES_EQUAL(result.PropositionBase->GetFingerprint(), updated.GetFingerprint());
        UNIT_ASSERT(result.ConfigToPropose->GetSelfManagementConfig().GetAutomaticBootstrap());
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetFingerprint(),
            fixture.Config.GetFingerprint());
    }

    Y_UNIT_TEST(RecoveryRejectsUnknownProposalLineage) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto intermediate = MakeNextConfig(fixture.Config);
        const auto proposal = MakeNextConfig(intermediate, 3);
        auto collected = MakeCollectedConfigs(fixture.Config, {2, 3});
        AddCollectedConfig(collected, fixture.Config, {2, 3});
        AddCollectedConfig(collected, proposal, {2}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(result.ErrorReason, "recovery accepted a proposal with an unproven intermediate configuration");
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 1);
    }

    Y_UNIT_TEST(RecoveryRetainsParentOfMinorityChild) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        auto parent = MakeNextConfig(fixture.Config);
        parent.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&parent);
        const auto child = MakeNextConfig(parent, 3);
        auto collected = MakeCollectedConfigs(fixture.Config);
        AddCollectedConfig(collected, fixture.Config, {1, 2, 3});
        AddCollectedConfig(collected, parent, {1}, false);
        AddCollectedConfig(collected, child, {2}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        UNIT_ASSERT(result.RequiresRecoveryProposition);
        UNIT_ASSERT(result.ConfigToPropose);
        UNIT_ASSERT_VALUES_EQUAL(result.ConfigToPropose->SerializeAsString(), parent.SerializeAsString());
    }

    Y_UNIT_TEST(RecoveryRejectsMissingParentOfMinorityChild) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto parent = MakeNextConfig(fixture.Config);
        const auto child = MakeNextConfig(parent, 3);
        auto collected = MakeCollectedConfigs(fixture.Config);
        AddCollectedConfig(collected, fixture.Config, {1, 2, 3});
        AddCollectedConfig(collected, child, {2}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(result.ErrorReason, "recovery discarded a persisted child's missing parent");
    }

    Y_UNIT_TEST(RecoveryRejectsNonBootstrapProposalsWithoutBase) {
        TCommitConfirmationFixture fixture(true);
        fixture.Config.SetGeneration(0);
        fixture.Config.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).Clear();
        }
        fixture.Start();
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 0);
        const auto bootstrap = MakeNextConfig(fixture.Config, 1);
        const auto parent = MakeNextConfig(bootstrap);
        const auto child = MakeNextConfig(parent, 3);
        for (const bool includeZeroBase : {false, true}) {
            auto collected = MakeCollectedConfigs(fixture.Config);
            if (includeZeroBase) {
                AddCollectedConfig(collected, fixture.Config, {1, 2, 3});
            }
            AddCollectedConfig(collected, parent, {1}, false);
            AddCollectedConfig(collected, child, {2}, false);

            const auto result = ProcessCollectedConfigs(fixture, collected, true);
            UNIT_ASSERT_C(result.ErrorReason, "recovery bootstrapped over persistent non-bootstrap proposals");
        }
    }

    Y_UNIT_TEST(RecoveryRejectsProposalWithObsoleteParent) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto committed = MakeNextConfig(fixture.Config);
        const auto proposal = MakeNextConfig(fixture.Config, 3);
        auto collected = MakeCollectedConfigs(fixture.Config, {2, 3});
        AddCollectedConfig(collected, committed, {2, 3});
        AddCollectedConfig(collected, proposal, {2}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(result.ErrorReason, "recovery accepted a higher proposal whose parent predates the proven quorum");
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 1);
    }

    Y_UNIT_TEST(RecoveryRejectsPossiblyChosenConflictingProposals) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto first = MakeNextConfig(fixture.Config);
        auto second = first;
        second.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&second);
        auto collected = MakeCollectedConfigs(fixture.Config, {2, 3});
        AddCollectedConfig(collected, fixture.Config, {2, 3});
        AddCollectedConfig(collected, first, {2}, false);
        AddCollectedConfig(collected, second, {3}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(result.ErrorReason, "recovery arbitrarily selected a possibly chosen conflicting proposal");
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 1);
    }

    Y_UNIT_TEST(RecoveryRejectsPossiblyChosenProposalSiblings) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        const auto first = MakeNextConfig(fixture.Config);
        auto second = MakeNextConfig(fixture.Config, 3);
        second.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&second);
        auto collected = MakeCollectedConfigs(fixture.Config, {2, 3});
        AddCollectedConfig(collected, fixture.Config, {2, 3});
        AddCollectedConfig(collected, first, {2}, false);
        AddCollectedConfig(collected, second, {3}, false);

        const auto result = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(result.ErrorReason, "a larger generation hid a possibly chosen proposal on a different branch");
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 1);
    }

    Y_UNIT_TEST(AdvanceCollectsMinorityProposal) {
        TCommitConfirmationFixture fixture(true);
        fixture.Start(true);
        SimUntil(fixture.Runtime, [] { return false; }, TDuration::Seconds(1));
        const ui32 sourceNodeId = fixture.Root().NodeId() % 3 + 1;
        fixture.Metadata(sourceNodeId).MutableProposedStorageConfig()->CopyFrom(MakeNextConfig(fixture.Config, 5));
        bool sawProposalRead = false;
        fixture.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId == sourceNodeId && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadataResult::EventType
                && ev->Get<NStorage::TEvNodeWardenReadMetadataResult>()->Record.GetProposedStorageConfig().GetGeneration() == 5) {
                sawProposalRead = true;
            }
            return true;
        };
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableAdvanceGeneration();
        InvokeKeeper(fixture.Runtime, fixture.Root(), std::move(request));
        UNIT_ASSERT(sawProposalRead);
        const auto advanced = QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(advanced.GetGeneration(), 6);
        CheckDurableConfigConverges(fixture, advanced);
    }

    Y_UNIT_TEST(EmptyReplicaWaitsBeforeVoting) {
        auto config = MakeStorageConfig(1, 1);
        config.MutableSelfManagementConfig()->SetEnabled(true);
        config.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        auto base = config;
        base.SetGeneration(0);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&base);
        TNodeWardenTestState state;
        TTestActorSystem runtime(1);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        const TActorId keeper = RegisterKeeper(runtime, base, 1, state);
        ui32 proposedWrites = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig().GetGeneration() == 2) {
                ++proposedWrites;
            }
            return true;
        };
        const auto propose = [&] {
            const TActorId edge = runtime.AllocateEdgeActor(1);
            NKikimrBlobStorage::TEvNodeConfigScatter request;
            request.SetTaskId(RandomNumber<ui64>());
            request.MutableProposeStorageConfig()->MutableConfig()->CopyFrom(MakeNextConfig(config));
            UNIT_ASSERT(runtime.WrapInActorContext(keeper, [&](IActor *actor) {
                static_cast<NStorage::TDistributedConfigKeeper*>(actor)->IssueScatterTask(TActorId(edge), std::move(request));
            }));
            return runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigGather>(edge);
        };

        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(10));
        const auto before = propose();
        UNIT_ASSERT_VALUES_EQUAL(before->Get()->Record.GetProposeStorageConfig().StatusSize(), 1);
        UNIT_ASSERT(before->Get()->Record.GetProposeStorageConfig().GetStatus(0).GetStatus()
                    != NKikimrBlobStorage::TEvNodeConfigGather::TProposeStorageConfig::ACCEPTED);
        UNIT_ASSERT_VALUES_EQUAL(proposedWrites, 0);

        UNIT_ASSERT(runtime.WrapInActorContext(keeper, [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyCommittedStorageConfig(config);
        }));
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return state.Metadata[std::pair<ui32, TString>{1, "/dev/disk1_1"}].HasCommittedStorageConfig();
        }, TDuration::Seconds(1)));
        const auto after = propose();
        UNIT_ASSERT(after->Get()->Record.GetProposeStorageConfig().GetStatus(0).GetStatus()
                    == NKikimrBlobStorage::TEvNodeConfigGather::TProposeStorageConfig::ACCEPTED);
        UNIT_ASSERT_VALUES_EQUAL(proposedWrites, 1);
    }

    Y_UNIT_TEST(CommittedProposalReplayPreservesAcceptedProposal) {
        auto committed = MakeStorageConfig(1, 1);
        committed.MutableSelfManagementConfig()->SetEnabled(true);
        committed.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&committed);
        auto base = committed;
        base.SetGeneration(0);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&base);
        TNodeWardenTestState state;
        auto& metadata = state.Metadata[std::pair<ui32, TString>{1, "/dev/disk1_1"}];
        metadata.MutableCommittedStorageConfig()->CopyFrom(committed);
        TTestActorSystem runtime(1);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        const TActorId keeper = RegisterKeeper(runtime, base, 1, state);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return state.NodeWardenConfigs.contains(1)
                && state.NodeWardenConfigs.at(1)->GetFingerprint() == committed.GetFingerprint();
        }));
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(10));
        bool holdWrites = false;
        THashSet<TActorId> replied;
        std::vector<std::unique_ptr<IEventHandle>> delayedWrites;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (holdWrites && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                delayedWrites.push_back(std::move(ev));
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType) {
                replied.insert(ev->GetRecipientRewrite());
            }
            return true;
        };
        const auto propose = [&](const NKikimrBlobStorage::TStorageConfig& config) {
            const TActorId edge = runtime.AllocateEdgeActor(1);
            NKikimrBlobStorage::TEvNodeConfigScatter request;
            request.SetTaskId(RandomNumber<ui64>());
            request.MutableProposeStorageConfig()->MutableConfig()->CopyFrom(config);
            UNIT_ASSERT(runtime.WrapInActorContext(keeper, [&](IActor *actor) {
                static_cast<NStorage::TDistributedConfigKeeper*>(actor)->IssueScatterTask(TActorId(edge), std::move(request));
            }));
            return edge;
        };
        const auto result = [&](TActorId edge) {
            const auto response = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigGather>(edge);
            const auto& statuses = response->Get()->Record.GetProposeStorageConfig().GetStatus();
            UNIT_ASSERT_VALUES_EQUAL(statuses.size(), 1);
            return statuses.Get(0);
        };
        using TPropose = NKikimrBlobStorage::TEvNodeConfigGather::TProposeStorageConfig;
        auto accepted = MakeNextConfig(committed);
        accepted.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&accepted);
        auto status = result(propose(accepted));
        UNIT_ASSERT(status.GetStatus() == TPropose::ACCEPTED);
        UNIT_ASSERT_VALUES_EQUAL(status.SuccessfulDrivesSize(), 1);
        const TString acceptedMetadata = metadata.SerializeAsString();
        holdWrites = true;
        const TActorId duplicateEdge = propose(committed);
        UNIT_ASSERT(SimUntil(runtime, [&] { return !delayedWrites.empty(); }));
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(10));
        UNIT_ASSERT(!replied.contains(duplicateEdge));
        UNIT_ASSERT_VALUES_EQUAL(metadata.SerializeAsString(), acceptedMetadata);
        UNIT_ASSERT(result(propose(committed)).GetStatus() == TPropose::RACE);
        holdWrites = false;
        ReplayDelayedEvents(runtime, delayedWrites);
        status = result(duplicateEdge);
        UNIT_ASSERT(status.GetStatus() == TPropose::ACCEPTED);
        UNIT_ASSERT_VALUES_EQUAL(status.SuccessfulDrivesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.SerializeAsString(), acceptedMetadata);

        holdWrites = true;
        UNIT_ASSERT(runtime.WrapInActorContext(keeper, [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyCommittedStorageConfig(accepted);
        }));
        UNIT_ASSERT(SimUntil(runtime, [&] { return !delayedWrites.empty(); }));
        UNIT_ASSERT(result(propose(accepted)).GetStatus() == TPropose::RACE);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetCommittedStorageConfig().GetGeneration(), 1);
        holdWrites = false;
        ReplayDelayedEvents(runtime, delayedWrites);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return metadata.GetCommittedStorageConfig().GetFingerprint() == accepted.GetFingerprint();
        }));
        UNIT_ASSERT(result(propose(committed)).GetStatus() == TPropose::ERROR);
        auto divergent = accepted;
        divergent.MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&divergent);
        UNIT_ASSERT(result(propose(divergent)).GetStatus() == TPropose::ERROR);
        const auto volatileConfig = MakeNextConfig(accepted, 3);
        UNIT_ASSERT(runtime.WrapInActorContext(keeper, [&](IActor *actor) {
            UNIT_ASSERT(static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyStorageConfig(volatileConfig));
        }));
        UNIT_ASSERT(result(propose(volatileConfig)).GetStatus() == TPropose::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetCommittedStorageConfig().GetFingerprint(), accepted.GetFingerprint());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetProposedStorageConfig().GetFingerprint(), accepted.GetFingerprint());
    }

    Y_UNIT_TEST(LegacyRefillUsesSavedPublication) {
        TCommitConfirmationFixture fixture(true);
        fixture.State.RequiredRootNodeId = 1;
        fixture.Metadata(3).Clear();
        const auto queryOnlyConfig = MakeNextConfig(fixture.Config);
        bool holdReply = true;
        ui32 ordinaryPublications = 0;
        ui32 queryBodyWrites = 0;
        std::vector<std::unique_ptr<IEventHandle>> pendingReplies;
        fixture.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (holdReply && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (record.HasQueryConfig() && record.GetQueryConfig().GetFresh()) {
                    record.MutableQueryConfig()->SetFresh(false);
                    record.MutableQueryConfig()->MutableConfig()->CopyFrom(queryOnlyConfig);
                    pendingReplies.push_back(std::move(ev));
                    return false;
                }
            }
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.HasCommittedStorageConfig()) {
                ++ordinaryPublications;
            }
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetCommittedStorageConfig().GetGeneration()
                   == queryOnlyConfig.GetGeneration()) {
                ++queryBodyWrites;
            }
            return true;
        };
        fixture.Start(true);
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return !pendingReplies.empty() && ordinaryPublications; }));
        SimUntil(fixture.Runtime, [] { return false; }, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(queryBodyWrites, 0);
        UNIT_ASSERT(!fixture.Metadata(3).HasCommittedStorageConfig());

        holdReply = false;
        ReplayDelayedEvents(fixture.Runtime, pendingReplies);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(queryBodyWrites, 0);
    }

    bool CaptureInvokeResult(IEventHandle& ev, TActorId edge,
                             std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult>& response) {
        if (ev.GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType
            && ev.GetRecipientRewrite() == edge) {
            response.emplace(ev.Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record);
            return true;
        }
        return false;
    }

    void StartZeroGenerationRoot(TCommitConfirmationFixture& fixture) {
        fixture.Config.SetGeneration(0);
        auto *selfManagement = fixture.Config.MutableSelfManagementConfig();
        selfManagement->SetAutomaticBootstrap(false);
        selfManagement->SetErasureSpecies("none");
        selfManagement->SetPDiskType(NKikimrBlobStorage::EPDiskType::ROT);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.Metadata.clear();
        fixture.Runtime.Start();
        for (const auto& node : fixture.Config.GetAllNodes()) {
            fixture.Keepers.push_back(RegisterKeeper(fixture.Runtime, fixture.Config, node.GetNodeId(), fixture.State,
                "metadata: {kind: MainConfig, version: 0, cluster: ''}\n"
                "config: {}\nallowed_labels: {}\nselector_config: []\n"));
        }
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] {
            return FindConvergedRoot(fixture.Runtime, fixture.Keepers) == 1;
        }));
        UNIT_ASSERT(QueryKeeper(fixture.Runtime, fixture.Root()).HasScepter());
        SimUntil(fixture.Runtime, [] { return false; }, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().GetGeneration(), 0);
    }

    Y_UNIT_TEST(SelfHealRefreshStartsAfterQuorumRecovery) {
        TCommitConfirmationFixture fixture(true);
        ui32 requests = 0;
        TActorId requester;
        fixture.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTabletPipe::EvSend
                    && ev->Type == NStorage::TEvNodeConfigGetSelfHealState::EventType) {
                ++requests;
                requester = ev->Sender;
                return false; // Observe the real outgoing request without running CMS.
            }
            return true;
        };
        StartZeroGenerationRoot(fixture);
        const auto scepter = QueryKeeper(fixture.Runtime, fixture.Root()).GetScepter().GetId();
        UNIT_ASSERT_VALUES_EQUAL(requests, 0);

        auto recovered = MakeNextConfig(fixture.Config, 1);
        auto *group = recovered.MutableStateStorageConfig()->AddRingGroups();
        group->SetNToSelect(3);
        for (ui32 node : {1, 2, 3}) {
            group->AddRing()->AddNode(node);
        }
        recovered.MutableStateStorageBoardConfig()->CopyFrom(recovered.GetStateStorageConfig());
        recovered.MutableSchemeBoardConfig()->CopyFrom(recovered.GetStateStorageConfig());
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&recovered);
        auto collected = MakeCollectedConfigs(fixture.Config);
        AddCollectedConfig(collected, recovered, {1, 2, 3});
        for (ui32 node : {1, 2, 3}) {
            fixture.Metadata(node).MutableCommittedStorageConfig()->CopyFrom(recovered);
        }
        const auto result = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return requests != 0; }));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetScepter().GetId(), scepter);

        // A disabled Sentinel completes this refresh. Subsequent configuration
        // changes in the same tenure must not start another refresh loop.
        auto disabled = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        disabled->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
        disabled->Record.SetErrorReason("Sentinel is disabled");
        fixture.Runtime.Send(new IEventHandle(requester, fixture.Root(), disabled.release()), requester.NodeId());
        const auto updated = MakeNextConfig(recovered, 2);
        UNIT_ASSERT(fixture.Runtime.WrapInActorContext(fixture.Root(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyCommittedStorageConfig(updated);
        }));
        UNIT_ASSERT_C(!SimUntil(fixture.Runtime, [&] { return requests > 1; }, TDuration::Seconds(15)),
            "a completed refresh restarted on an ordinary configuration update");
    }

    Y_UNIT_TEST(PublishedConfigFencesEmptyRecovery) {
        TCommitConfirmationFixture fixture(true);
        StartZeroGenerationRoot(fixture);
        auto collected = MakeCollectedConfigs(fixture.Config);
        for (const auto& node : fixture.Config.GetAllNodes()) {
            auto *disk = collected.AddNoMetadata();
            disk->MutableNodeId()->CopyFrom(node);
            disk->SetPath("/dev/disk" + std::to_string(node.GetNodeId()) + "_1");
        }
        UNIT_ASSERT(!ProcessCollectedConfigs(fixture, collected, true).ErrorReason);
        const auto published = MakeNextConfig(fixture.Config, 1);
        UNIT_ASSERT(fixture.Runtime.WrapInActorContext(fixture.Root(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->UpdateBound(
                2, fixture.Config.GetAllNodes(1), published, nullptr);
        }));
        const auto missing = ProcessCollectedConfigs(fixture, collected, true);
        UNIT_ASSERT_C(missing.ErrorReason, "known applied configuration was ignored by a generation-zero root");
        UNIT_ASSERT(!missing.ConfigToPropose);

        collected = MakeCollectedConfigs(fixture.Config);
        AddCollectedConfig(collected, published, {1, 2, 3}, false);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).MutableProposedStorageConfig()->CopyFrom(published);
        }
        const auto recovered = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!recovered.ErrorReason, recovered.ErrorReason.value_or(""));
        UNIT_ASSERT(recovered.RecoveredConfig);
        UNIT_ASSERT_VALUES_EQUAL(recovered.RecoveredConfig->GetFingerprint(), published.GetFingerprint());
        CheckDurableConfigConverges(fixture, published);
    }

    void CheckBootstrapMinorityProposals(bool restartWithConflictingProposal) {
        TCommitConfirmationFixture fixture(true);
        StartZeroGenerationRoot(fixture);
        const TActorId root = fixture.Root();
        bool failWrites = true;
        ui32 writableNode = 2;
        TString rejectedFingerprint;
        fixture.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (failWrites && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record;
                if (nodeId != writableNode || record.GetProposedStorageConfig().GetFingerprint() == rejectedFingerprint) {
                    fixture.Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new NStorage::TEvNodeWardenWriteMetadataResult(std::nullopt, NPDisk::EPDiskMetadataOutcome::ERROR),
                        0, ev->Cookie), nodeId);
                    return false;
                }
            }
            return true;
        };
        const auto failBootstrap = [&] {
            const TActorId edge = fixture.Runtime.AllocateEdgeActor(1);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableBootstrapCluster()->SetSelfAssemblyUUID("partial-bootstrap-test");
            fixture.Runtime.Send(new IEventHandle(root, edge, request.release()), 1);
            const auto response = fixture.Runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
            UNIT_ASSERT(response->Get()->Record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        };
        failBootstrap();
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Metadata.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetProposedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT(!fixture.Metadata(2).HasCommittedStorageConfig());
        if (restartWithConflictingProposal) {
            rejectedFingerprint = fixture.Metadata(2).GetProposedStorageConfig().GetFingerprint();
            writableNode = 3;
            failBootstrap();
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Metadata.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetProposedStorageConfig().GetFingerprint(), rejectedFingerprint);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetProposedStorageConfig().GetGeneration(), 1);
            UNIT_ASSERT(fixture.Metadata(3).GetProposedStorageConfig().GetFingerprint() != rejectedFingerprint);
            for (const auto& [disk, record] : fixture.State.Metadata) {
                UNIT_ASSERT(!record.HasCommittedStorageConfig());
            }
            for (ui32 nodeId : {1, 2, 3}) {
                fixture.Runtime.StopNode(nodeId);
            }
            failWrites = false;
            for (ui32 nodeId : {1, 2, 3}) {
                fixture.Runtime.StartNode(nodeId);
                fixture.Keepers[nodeId - 1] = RegisterKeeper(fixture.Runtime, fixture.Config, nodeId, fixture.State,
                    "metadata: {kind: MainConfig, version: 0, cluster: ''}\n"
                    "config: {}\nallowed_labels: {}\nselector_config: []\n");
            }
            UNIT_ASSERT(SimUntil(fixture.Runtime, [&] {
                return FindConvergedRoot(fixture.Runtime, fixture.Keepers) == 1;
            }));
        }
        failWrites = false;
        BootstrapCluster(fixture.Runtime, fixture.Root());
        const auto committed = QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(committed.GetSelfAssemblyUUID(), "partial-bootstrap-test");
        CheckDurableConfigConverges(fixture, committed);
    }

    Y_UNIT_TEST(BootstrapIgnoresMinorityProposal) {
        CheckBootstrapMinorityProposals(false);
    }

    Y_UNIT_TEST(BootstrapRetriesConflictingProposals) {
        CheckBootstrapMinorityProposals(true);
    }

    Y_UNIT_TEST(BootstrapReproposesPossiblyChosenConfig) {
        TCommitConfirmationFixture fixture(true);
        auto committed = fixture.Config;
        committed.SetSelfAssemblyUUID("existing-bootstrap");
        const auto yaml = fixture.MakeReplacementYaml(committed, 0, "none");
        const auto compositeError = NStorage::TDistributedConfigKeeper::UpdateConfigComposite(committed, yaml, yaml);
        UNIT_ASSERT_C(!compositeError, compositeError.value_or(""));
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&committed);
        auto accepted = MakeNextConfig(committed);
        accepted.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&accepted);

        std::array<ui32, 4> reads{};
        ui32 failedAutomaticReads = 0;
        ui32 recoveryProposals = 0;
        bool allowCollection = false;
        bool allowThirdRead = false;
        bool holdRecovery = true;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedWrites;
        fixture.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                if (++reads[nodeId] == 1) {
                    if (nodeId != 1) {
                        fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(committed);
                        if (nodeId == 2) {
                            fixture.Metadata(nodeId).MutableProposedStorageConfig()->CopyFrom(accepted);
                        }
                    }
                } else if (!allowCollection || (nodeId == 3 && !allowThirdRead)) {
                    failedAutomaticReads += !allowCollection;
                    fixture.Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new NStorage::TEvNodeWardenReadMetadataResult(std::nullopt, NPDisk::EPDiskMetadataOutcome::ERROR,
                            NKikimrBlobStorage::TPDiskMetadataRecord()), 0, ev->Cookie), nodeId);
                    return false;
                }
            } else if (type == NStorage::TEvNodeConfigScatter::EventType) {
                const auto& scatter = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (allowCollection && scatter.HasProposeStorageConfig()) {
                    const auto& proposed = scatter.GetProposeStorageConfig().GetConfig();
                    ++recoveryProposals;
                    UNIT_ASSERT_VALUES_EQUAL(proposed.GetGeneration(), accepted.GetGeneration());
                    UNIT_ASSERT_VALUES_EQUAL(proposed.GetPrevConfig().GetFingerprint(), committed.GetFingerprint());
                    UNIT_ASSERT_VALUES_EQUAL(proposed.GetFingerprint(), accepted.GetFingerprint());
                    UNIT_ASSERT_VALUES_EQUAL(proposed.SerializeAsString(), accepted.SerializeAsString());
                }
            } else if (holdRecovery && nodeId == 2 && type == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record;
                if (record.GetProposedStorageConfig().GetGeneration() == accepted.GetGeneration()) {
                    UNIT_ASSERT_VALUES_EQUAL(record.GetProposedStorageConfig().SerializeAsString(), accepted.SerializeAsString());
                    delayedWrites.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        StartZeroGenerationRoot(fixture);
        UNIT_ASSERT(failedAutomaticReads);
        fixture.Metadata(1).MutableCommittedStorageConfig()->CopyFrom(committed);
        allowCollection = true;
        edge = fixture.Runtime.AllocateEdgeActor(1);
        const auto invokeBootstrap = [&](const TString& uuid) {
            response.reset();
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableBootstrapCluster()->SetSelfAssemblyUUID(uuid);
            fixture.Runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
        };
        invokeBootstrap("different-bootstrap");
        UNIT_ASSERT_C(SimUntil(fixture.Runtime, [&] { return !delayedWrites.empty() || response; }),
            "bootstrap did not start recovery of the persisted proposal");
        UNIT_ASSERT_C(!response.has_value(), (response ? response->DebugString() : TString()));
        UNIT_ASSERT(recoveryProposals);
        UNIT_ASSERT(!delayedWrites.empty());
        SimUntil(fixture.Runtime, [] { return false; }, TDuration::MilliSeconds(100));
        UNIT_ASSERT(!response.has_value());
        holdRecovery = false;
        ReplayDelayedEvents(fixture.Runtime, delayedWrites);
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return response.has_value(); }));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(response->GetStatus() == TResult::RACE, response->DebugString());
        allowThirdRead = true;
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, fixture.Root()).GetQueryConfig().GetConfig().SerializeAsString(),
            accepted.SerializeAsString());
        invokeBootstrap(accepted.GetSelfAssemblyUUID());
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return response.has_value(); }));
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        invokeBootstrap("different-bootstrap");
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return response.has_value(); }));
        UNIT_ASSERT_C(response->GetStatus() == TResult::ERROR, response->DebugString());
        CheckDurableConfigConverges(fixture, accepted);
        for (ui32 nodeId : {1, 2, 3}) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().SerializeAsString(),
                accepted.SerializeAsString());
        }
    }

    Y_UNIT_TEST(ZeroRootCollectsPublishedConfig) {
        TCommitConfirmationFixture fixture(true);
        StartZeroGenerationRoot(fixture);
        bool sawRead = false;
        fixture.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            sawRead |= nodeId == 1 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType;
            return true;
        };
        const auto published = MakeNextConfig(fixture.Config, 1);
        UNIT_ASSERT(fixture.Runtime.WrapInActorContext(fixture.Root(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->UpdateBound(
                2, fixture.Config.GetAllNodes(1), published, nullptr);
        }));
        UNIT_ASSERT_C(SimUntil(fixture.Runtime, [&] { return sawRead; }, TDuration::Seconds(3)),
            "applied metadata did not trigger generation-zero configuration recovery");
    }

    Y_UNIT_TEST(PublishedConfigFencesBootstrap) {
        TCommitConfirmationFixture fixture(true);
        StartZeroGenerationRoot(fixture);
        const TActorId root = fixture.Root();
        const TActorId edge = fixture.Runtime.AllocateEdgeActor(1);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> result;
        bool holdGathers = true;
        std::vector<std::unique_ptr<IEventHandle>> heldGathers;
        fixture.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (CaptureInvokeResult(*ev, edge, result)) {
                return false;
            }
            if (holdGathers && ev->GetRecipientRewrite() == root
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType
                && ev->Get<NStorage::TEvNodeConfigGather>()->Record.HasProposeStorageConfig()) {
                heldGathers.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableBootstrapCluster()->SetSelfAssemblyUUID("pending-bootstrap");
        fixture.Runtime.Send(new IEventHandle(root, edge, request.release()), 1);
        UNIT_ASSERT_C(SimUntil(fixture.Runtime, [&] { return !heldGathers.empty(); }),
            "bootstrap did not reach the pending-proposition boundary");
        auto published = MakeNextConfig(fixture.Config, 1);
        published.SetSelfAssemblyUUID("already-applied-bootstrap");
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&published);
        UNIT_ASSERT(fixture.Runtime.WrapInActorContext(root, [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->UpdateBound(
                2, fixture.Config.GetAllNodes(1), published, nullptr);
        }));
        holdGathers = false;
        ReplayDelayedEvents(fixture.Runtime, heldGathers);
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return result.has_value(); }));
        UNIT_ASSERT(result->GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(fixture.Runtime, root).GetQueryConfig().GetConfig().GetGeneration(), 0);
        UNIT_ASSERT(!fixture.Metadata(1).HasCommittedStorageConfig());
    }

    bool ReplyToReplaceController(TTestActorSystem& runtime, ui32 nodeId, IEventHandle& ev, TActorId sender) {
        if (ev.GetTypeRewrite() != TEvTabletPipe::EvSend) {
            return false;
        }
        if (ev.Type == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
            auto result = std::make_unique<TEvBlobStorage::TEvControllerConfigResponse>();
            result->Record.MutableResponse()->AddStatus()->SetInterfaceVersion(BSC_INTERFACE_DISTCONF_CONTROL);
            runtime.Send(new IEventHandle(ev.Sender, sender, result.release()), nodeId);
        } else if (ev.Type == TEvBlobStorage::TEvControllerDistconfRequest::EventType) {
            auto result = std::make_unique<TEvBlobStorage::TEvControllerDistconfResponse>();
            result->Record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
            runtime.Send(new IEventHandle(ev.Sender, sender, result.release()), nodeId);
        } else {
            return false;
        }
        return true;
    }

    void SetMetadataDiskGuid(ui32 nodeId, IEventHandle& ev) {
        if (ev.GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadataResult::EventType) {
            ev.Get<NStorage::TEvNodeWardenWriteMetadataResult>()->Guid = nodeId * 1000 + 1;
        } else if (ev.GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadataResult::EventType) {
            ev.Get<NStorage::TEvNodeWardenReadMetadataResult>()->Guid = nodeId * 1000 + 1;
        }
    }

    bool HandleReplaceEvent(TTestActorSystem& runtime, ui32 nodeId, IEventHandle& ev,
                            TActorId edge, std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult>& response) {
        SetMetadataDiskGuid(nodeId, ev);
        return ReplyToReplaceController(runtime, nodeId, ev, edge)
            || (edge && CaptureInvokeResult(ev, edge, response));
    }

    void CheckDisableDistconfVersion(ui64 version) {
        TCommitConfirmationFixture fixture(true);
        fixture.State.RequiredRootNodeId = 1;
        const auto currentYaml = fixture.MakeReplacementYaml(4);
        const auto error = NStorage::TDistributedConfigKeeper::UpdateConfigComposite(fixture.Config, currentYaml, currentYaml);
        UNIT_ASSERT_C(!error, error.value_or(""));
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        fixture.Start();
        auto& runtime = fixture.Runtime;
        const TActorId edge = runtime.AllocateEdgeActor(1);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::optional<NKikimrBlobStorage::TStorageConfig> disabledCommittedConfig;
        ui32 disableRequests = 0;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType
                    && ev->Recipient == MakeBlobStorageNodeWardenID(1)) {
                const auto& msg = *ev->Get<TEvNodeWardenStorageConfig>();
                if (!msg.SelfManagementEnabled) {
                    UNIT_ASSERT_VALUES_EQUAL(msg.Config->GetFingerprint(), fixture.Config.GetFingerprint());
                    if (msg.CommittedConfig) {
                        disabledCommittedConfig = *msg.CommittedConfig;
                    }
                }
            }
            if (ev->GetTypeRewrite() == TEvTabletPipe::EvSend
                    && ev->Type == TEvBlobStorage::TEvControllerDistconfRequest::EventType) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerDistconfRequest>()->Record;
                UNIT_ASSERT_C(record.GetOperation() ==
                    NKikimrBlobStorage::TEvControllerDistconfRequest::DisableDistconf, record.DebugString());
                UNIT_ASSERT(record.HasCompressedMainConfig());
                UNIT_ASSERT(!record.GetDedicatedConfigMode());
                UNIT_ASSERT(!record.GetDryRun());
                ++disableRequests;
            }
            return !HandleReplaceEvent(runtime, nodeId, *ev, edge, response);
        };
        auto request = fixture.MakeReplaceRequest(version);
        TString yaml = request->Record.GetReplaceStorageConfig().GetYAML();
        const TStringBuf enabled = "enabled: true";
        const size_t pos = yaml.find(enabled);
        UNIT_ASSERT(pos != TString::npos);
        yaml.replace(pos, enabled.size(), "enabled: false");
        request->Record.MutableReplaceStorageConfig()->SetYAML(yaml);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        if (version != 5) {
            UNIT_ASSERT_C(response->GetStatus() == TResult::ERROR, response->DebugString());
            UNIT_ASSERT_STRING_CONTAINS(response->GetErrorReason(), "Main config version must be increasing by one");
            UNIT_ASSERT_VALUES_EQUAL(disableRequests, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetFingerprint(),
                fixture.Config.GetFingerprint());
        } else {
            UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(disableRequests, 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] {
                return disabledCommittedConfig
                    && disabledCommittedConfig->GetGeneration() == fixture.Config.GetGeneration() + 1
                    && !disabledCommittedConfig->GetSelfManagementConfig().GetEnabled();
            }), (disabledCommittedConfig ? disabledCommittedConfig->DebugString() : "missing disabled notification"));
            const auto applied = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(applied.GetGeneration(), fixture.Config.GetGeneration() + 1);
            UNIT_ASSERT(!applied.GetSelfManagementConfig().GetEnabled());
            UNIT_ASSERT_VALUES_EQUAL(applied.GetFingerprint(), disabledCommittedConfig->GetFingerprint());
        }
    }

    Y_UNIT_TEST(DisableDistconfRejectsFutureYamlVersionBeforeBscMutation) {
        CheckDisableDistconfVersion(14);
    }

    Y_UNIT_TEST(DisableDistconfAcceptsNextYamlVersion) {
        CheckDisableDistconfVersion(5);
    }

    bool HasTestConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config,
                            const std::vector<ui32>& nodeIds, const NKikimrConfig::TBridgeConfig *bridge = nullptr) {
        std::vector<NStorage::TSuccessfulDisk> disks;
        for (ui32 nodeId : nodeIds) {
            disks.emplace_back(NStorage::TNodeIdentifier(config.GetAllNodes(nodeId - 1)),
                "/dev/disk" + std::to_string(nodeId) + "_1", nodeId * 1000 + 1);
        }
        return NStorage::HasConfigQuorum(config, disks, TBridgePileId(),
            *MakeNodeWardenConfig(config, bridge), true);
    }

    void CheckAcknowledgedUpdateSurvivesQuorumTurnover(bool fragmentWithChild) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 1}};
        auto& runtime = fixture.Runtime;
        bool holdPublication = true;
        bool recovering = false;
        bool holdRecoveryWrite = true;
        bool sawRecoveryWriteOnB = false;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedPublications;
        std::vector<std::unique_ptr<IEventHandle>> delayedCommittedWrites;
        std::vector<std::unique_ptr<IEventHandle>> delayedRecoveryWrites;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record;
                if (!recovering && nodeId == 1 && (record.GetCommittedStorageConfig().GetGeneration() == 2
                    || (fragmentWithChild && record.GetProposedStorageConfig().GetGeneration() >= 3))) {
                    delayedCommittedWrites.push_back(std::move(ev));
                    return false;
                }
                if (recovering && holdRecoveryWrite && record.GetProposedStorageConfig().GetGeneration() >= 2) {
                    if (nodeId == 3) {
                        delayedRecoveryWrites.push_back(std::move(ev));
                        return false;
                    }
                    sawRecoveryWriteOnB |= nodeId == 2;
                }
            }
            if (holdPublication && nodeId == 2
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetCommittedStorageConfig().GetGeneration() == 2) {
                delayedPublications.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {1, 2}));
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {2, 3}));
        runtime.StopNode(3);
        edge = runtime.AllocateEdgeActor(1);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto *updated = request->Record.MutableUpdateConfig()->MutableConfig();
        updated->CopyFrom(fixture.Config);
        updated->MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(65)),
            "the original update did not return a result");
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return !delayedPublications.empty() && !delayedCommittedWrites.empty();
        }), "the acknowledged update did not reach its delayed commit publication");
        const auto accepted = fixture.Metadata(1).GetProposedStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(accepted.GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetProposedStorageConfig().GetGeneration(), 2);
        UNIT_ASSERT(fixture.Metadata(2).GetProposedStorageConfig().GetSelfManagementConfig().GetAutomaticBootstrap());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);

        if (fragmentWithChild) {
            response.reset();
            auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            advance->Record.MutableAdvanceGeneration();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, advance.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] {
                return fixture.Metadata(2).GetProposedStorageConfig().GetGeneration() > 2;
            }, TDuration::Seconds(65)), "the next proposal did not replace B's accepted parent");
            UNIT_ASSERT(!response || response->GetStatus() != TResult::OK);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetProposedStorageConfig().SerializeAsString(),
                accepted.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetProposedStorageConfig().GetPrevConfig().GetFingerprint(),
                accepted.GetFingerprint());
        }
        runtime.StopNode(1);
        recovering = true;
        fixture.State.RequiredRootNodeId = 2;
        fixture.State.RequiredParentNodeIds = {{2, 0}, {3, 2}};
        if (!fragmentWithChild) {
            runtime.StartNode(3);
            fixture.Keepers[2] = RegisterKeeper(runtime, fixture.Config, 3, fixture.State);
            UNIT_ASSERT_C(SimUntil(runtime, [&] {
                return sawRecoveryWriteOnB && !delayedRecoveryWrites.empty();
            }, TDuration::Seconds(90), 2), "B and C did not start proposal recovery after quorum turnover");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetProposedStorageConfig().SerializeAsString(),
                accepted.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers[1]).GetQueryConfig().GetConfig().GetGeneration(), 1);
            runtime.StopNode(3);
        }
        runtime.StopNode(2);
        delayedPublications.clear();
        delayedCommittedWrites.clear();
        delayedRecoveryWrites.clear();
        holdPublication = false;
        holdRecoveryWrite = false;
        fixture.State.RequiredParentNodeIds = {{1, 2}, {2, 0}, {3, 2}};
        for (ui32 nodeId : {1, 2, 3}) {
            runtime.StartNode(nodeId);
            fixture.Keepers[nodeId - 1] = RegisterKeeper(runtime, fixture.Config, nodeId, fixture.State);
        }
        const std::vector<TActorId> survivors = fixture.Keepers;
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, survivors) == 2; },
            TDuration::Seconds(90), 2), "the restarted cluster did not elect a root");
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers[1], true).HasScepter());

        edge = runtime.AllocateEdgeActor(2);
        for (ui32 attempt = 0; attempt != 3; ++attempt) {
            response.reset();
            auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            advance->Record.MutableAdvanceGeneration();
            runtime.Send(new IEventHandle(fixture.Keepers[1], edge, advance.release()), 2);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(65), 2),
                "the new root did not complete AdvanceGeneration");
            if (response->GetStatus() == TResult::OK) {
                break;
            }
            UNIT_ASSERT_C(response->GetStatus() == TResult::RACE, response->DebugString());
        }
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers[1]).GetQueryConfig().GetConfig();
        UNIT_ASSERT(winner.GetGeneration() > 2);
        UNIT_ASSERT_C(winner.GetSelfManagementConfig().GetAutomaticBootstrap(),
            "the successful post-failover mutation lost the body of the acknowledged update");
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(survivors, [&](TActorId id) {
                return fixture.Metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint();
            });
        }, TDuration::Seconds(30), 2), "the surviving quorum did not persist the recovered body");

        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(AcknowledgedUpdateSurvivesQuorumTurnover) {
        CheckAcknowledgedUpdateSurvivesQuorumTurnover(false);
    }

    Y_UNIT_TEST(AcknowledgedUpdateSurvivesMinorityChildProposal) {
        CheckAcknowledgedUpdateSurvivesQuorumTurnover(true);
    }

    Y_UNIT_TEST(RecoveryDoesNotPublishUnchosenProposalBeforeQuorum) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 1}};
        auto& runtime = fixture.Runtime;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        bool holdOriginalWrite = true;
        bool holdRecoveryWrite = true;
        bool publishedUnchosen = false;
        ui32 droppedOriginalWrites = 0;
        std::vector<std::unique_ptr<IEventHandle>> heldRecoveryWrites;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            if (ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                publishedUnchosen |= holdRecoveryWrite
                    && ev->Get<TEvNodeWardenStorageConfig>()->Config->GetSelfManagementConfig().GetAutomaticBootstrap();
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record;
                if (holdOriginalWrite && nodeId == 1 && record.GetProposedStorageConfig().GetGeneration() == 2) {
                    ++droppedOriginalWrites;
                    return false;
                }
                if (holdRecoveryWrite && nodeId == 3 && record.GetProposedStorageConfig().GetGeneration() == 2) {
                    heldRecoveryWrites.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        runtime.StopNode(3);
        edge = runtime.AllocateEdgeActor(1);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableUpdateConfig()->MutableConfig()->CopyFrom(fixture.Config);
        request->Record.MutableUpdateConfig()->MutableConfig()->MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        runtime.Send(new IEventHandle(fixture.Keepers[0], edge, request.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return droppedOriginalWrites && fixture.Metadata(2).GetProposedStorageConfig().GetGeneration() == 2;
        }), "the original proposal was not accepted on B alone");
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT(!response || response->GetStatus() != TResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT(!fixture.Metadata(1).HasProposedStorageConfig());

        runtime.StopNode(1);
        holdOriginalWrite = false;
        fixture.State.RequiredRootNodeId = 2;
        fixture.State.RequiredParentNodeIds = {{2, 0}, {3, 2}};
        runtime.StartNode(3);
        fixture.Keepers[2] = RegisterKeeper(runtime, fixture.Config, 3, fixture.State);
        const std::vector<TActorId> secondQuorum{fixture.Keepers[1], fixture.Keepers[2]};
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, secondQuorum) == 2; },
            TDuration::Seconds(90), 2));
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return publishedUnchosen || !heldRecoveryWrites.empty(); },
            TDuration::Seconds(30), 2), "recovery did not try to persist the possibly chosen body on a fresh quorum");
        UNIT_ASSERT_C(!publishedUnchosen, "recovery published an unchosen proposal before obtaining a fresh quorum");
        UNIT_ASSERT(!heldRecoveryWrites.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);

        runtime.StopNode(2);
        runtime.StopNode(3);
        heldRecoveryWrites.clear();
        holdRecoveryWrite = false;
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {3, 1}};
        runtime.StartNode(1);
        runtime.StartNode(3);
        fixture.Keepers[0] = RegisterKeeper(runtime, fixture.Config, 1, fixture.State);
        fixture.Keepers[2] = RegisterKeeper(runtime, fixture.Config, 3, fixture.State);
        const std::vector<TActorId> thirdQuorum{fixture.Keepers[0], fixture.Keepers[2]};
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, thirdQuorum) == 1; }));
        edge = runtime.AllocateEdgeActor(1);
        response.reset();
        auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        advance->Record.MutableAdvanceGeneration();
        runtime.Send(new IEventHandle(fixture.Keepers[0], edge, advance.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(65)));
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers[0]).GetQueryConfig().GetConfig();
        UNIT_ASSERT(!winner.GetSelfManagementConfig().GetAutomaticBootstrap());
        UNIT_ASSERT(!publishedUnchosen);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return std::ranges::all_of(thirdQuorum, [&](TActorId id) {
                return fixture.Metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint();
            });
        }));
    }

    enum class EFreshStorageMembershipScenario {
        ReplayAfterFormat,
        AdmissionReplyAfterFormat,
        RootLossDuringAdmission,
    };

    void CheckReplaceAddsFreshStorageNodesWithMirror3dc(EFreshStorageMembershipScenario scenario) {
        const bool delayAdmissionAfterFormat = scenario == EFreshStorageMembershipScenario::AdmissionReplyAfterFormat;
        const bool rootLossDuringAdmission = scenario == EFreshStorageMembershipScenario::RootLossDuringAdmission;
        TCommitConfirmationFixture fixture(false, 6);
        fixture.UseMirror3dcGroup();
        fixture.Config.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }

        auto expanded = MakeStorageConfig(6, 3, {1, 2, 3}, TBlobStorageGroupType::ErasureMirror3dc);
        expanded.MutableSelfManagementConfig()->CopyFrom(fixture.Config.GetSelfManagementConfig());
        for (ui32 nodeId : {4, 5, 6}) {
            expanded.MutableAllNodes(nodeId - 1)->MutableLocation()->CopyFrom(
                fixture.Config.GetAllNodes(nodeId - 4).GetLocation());
        }
        expanded.MutablePrevConfig()->CopyFrom(fixture.Config);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&expanded);

        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {1, 2, 3}));
        UNIT_ASSERT(!HasTestConfigQuorum(expanded, {1, 2, 3}));
        UNIT_ASSERT(HasTestConfigQuorum(expanded, {1, 2, 3, 4, 5, 6}));

        auto& runtime = fixture.Runtime;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        THashSet<ui32> targetedNodes;
        THashSet<ui32> proposedOnFreshNodes;
        std::optional<NKikimrBlobStorage::TEvNodeConfigScatter> savedTargeted;
        TActorId targetedSender;
        ui64 targetedCookie = 0;
        bool formatted = false;
        bool sawAdmissionRejection = false;
        ui32 proposedWritesAfterFormat = 0;
        bool holdAdmissionReplies = true;
        std::vector<std::unique_ptr<IEventHandle>> delayedAdmissions;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, edge, response)) {
                return false;
            }
            if (nodeId >= 4 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (record.GetTargeted() && record.HasProposeStorageConfig()) {
                    targetedNodes.insert(nodeId);
                    if (nodeId == 4 && !savedTargeted) {
                        savedTargeted.emplace(record);
                        targetedSender = ev->Sender;
                        targetedCookie = ev->Cookie;
                    }
                }
            }
            if (nodeId >= 4 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig().GetGeneration() == 3) {
                proposedOnFreshNodes.insert(nodeId);
                proposedWritesAfterFormat += formatted && nodeId == 4;
            }
            if (nodeId >= 4 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                sawAdmissionRejection |= formatted && nodeId == 4 && record.HasValidateTargetedProposition()
                    && !record.GetValidateTargetedProposition().GetConfirmed();
                if (!formatted && holdAdmissionReplies
                    && ((delayAdmissionAfterFormat && nodeId == 4) || rootLossDuringAdmission)
                    && record.HasValidateTargetedProposition()
                    && record.GetValidateTargetedProposition().GetConfirmed()) {
                    delayedAdmissions.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        fixture.Start(true);

        auto base = expanded;
        base.SetGeneration(0);
        base.ClearPrevConfig();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&base);
        const TString yaml = fixture.MakeReplacementYaml(expanded, 0, "mirror-3-dc");
        for (ui32 nodeId : {4, 5, 6}) {
            fixture.Keepers.push_back(RegisterKeeper(runtime, base, nodeId, fixture.State, yaml));
        }
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(100));
        for (ui32 nodeId : {4, 5, 6}) {
            UNIT_ASSERT(!fixture.Metadata(nodeId).HasCommittedStorageConfig());
        }

        edge = runtime.AllocateEdgeActor(1);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableReplaceStorageConfig()->SetYAML(yaml);
        request->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(true);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
        if (rootLossDuringAdmission) {
            UNIT_ASSERT_C(SimUntil(runtime, [&] {
                return targetedNodes.size() == 3 && delayedAdmissions.size() == 3;
            }), "new nodes did not obtain their admission grants");
            fixture.Disconnect(1, 2);
            fixture.Disconnect(1, 3);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }),
                          "replace did not abort after losing its root quorum");
            UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE,
                          response->DebugString());
            UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
            for (ui32 nodeId : {4, 5, 6}) {
                UNIT_ASSERT(!fixture.Metadata(nodeId).HasCommittedStorageConfig());
            }

            holdAdmissionReplies = false;
            ReplayDelayedEvents(runtime, delayedAdmissions);
            SimUntil(runtime, [] { return false; }, TDuration::Seconds(2));
            UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
            for (ui32 nodeId : {4, 5, 6}) {
                UNIT_ASSERT(!fixture.Metadata(nodeId).HasCommittedStorageConfig());
            }
            return;
        }
        if (delayAdmissionAfterFormat) {
            UNIT_ASSERT(SimUntil(runtime, [&] {
                return !delayedAdmissions.empty() && proposedOnFreshNodes.contains(5) && proposedOnFreshNodes.contains(6);
            }));
            SimUntil(runtime, [] { return false; }, TDuration::Seconds(4));
            runtime.StopNode(4);
            runtime.StartNode(4);
            fixture.Keepers[3] = RegisterKeeper(runtime, base, 4, fixture.State, yaml);
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)),
                      "membership expansion did not finish");
        UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      response->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(targetedNodes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(proposedOnFreshNodes.size(), delayAdmissionAfterFormat ? 2 : 3);

        const auto committed = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(committed.GetGeneration(), 3);
        UNIT_ASSERT_VALUES_EQUAL(committed.AllNodesSize(), 6);
        UNIT_ASSERT_VALUES_EQUAL(committed.GetBlobStorageConfig().GetServiceSet().GetGroups(0).SerializeAsString(),
                                 fixture.Config.GetBlobStorageConfig().GetServiceSet().GetGroups(0).SerializeAsString());
        CheckDurableConfigConverges(fixture, committed);
        fixture.State.RequiredParentNodeIds[4] = 0;
        runtime.StopNode(4);
        fixture.Metadata(4).Clear();
        runtime.StartNode(4);
        fixture.Keepers[3] = RegisterKeeper(runtime, base, 4, fixture.State, yaml);
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(100));
        UNIT_ASSERT(!fixture.Metadata(4).HasCommittedStorageConfig());
        UNIT_ASSERT(savedTargeted);
        formatted = true;
        if (delayAdmissionAfterFormat) {
            UNIT_ASSERT(!delayedAdmissions.empty());
            ReplayDelayedEvents(runtime, delayedAdmissions);
            SimUntil(runtime, [] { return false; }, TDuration::Seconds(2));
            UNIT_ASSERT_VALUES_EQUAL(proposedWritesAfterFormat, 0);
            UNIT_ASSERT(!fixture.Metadata(4).HasProposedStorageConfig());
            UNIT_ASSERT(!fixture.Metadata(4).HasCommittedStorageConfig());
            return;
        }
        auto stale = std::make_unique<NStorage::TEvNodeConfigScatter>();
        stale->Record.CopyFrom(*savedTargeted);
        runtime.Send(new IEventHandle(MakeBlobStorageNodeWardenID(4), targetedSender,
            stale.release(), 0, targetedCookie), targetedSender.NodeId());
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return sawAdmissionRejection || proposedWritesAfterFormat; }),
                      "replayed targeted proposal did not receive an admission decision");
        UNIT_ASSERT_VALUES_EQUAL(proposedWritesAfterFormat, 0);
        UNIT_ASSERT(sawAdmissionRejection);
        UNIT_ASSERT(!fixture.Metadata(4).HasProposedStorageConfig());
        UNIT_ASSERT(!fixture.Metadata(4).HasCommittedStorageConfig());
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(proposedWritesAfterFormat, 0);
    }

    Y_UNIT_TEST(RepeatReplaceAfterExpansionRestart) {
        TCommitConfirmationFixture fixture(false, 6);
        fixture.UseMirror3dcGroup();
        fixture.Config.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        fixture.Start(true);
        CheckDurableConfigConverges(fixture, fixture.Config);

        auto expanded = MakeStorageConfig(6, 3, {1, 2, 3}, TBlobStorageGroupType::ErasureMirror3dc);
        expanded.MutableSelfManagementConfig()->CopyFrom(fixture.Config.GetSelfManagementConfig());
        for (auto& node : *expanded.MutableAllNodes()) {
            node.MutableLocation()->CopyFrom(fixture.Config.GetAllNodes((node.GetNodeId() - 1) % 3).GetLocation());
        }
        expanded.MutablePrevConfig()->CopyFrom(fixture.Config);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&expanded);

        auto& runtime = fixture.Runtime;
        bool rejectedBinding = false;
        bool rejectRecoveryWrites = true;
        ui32 rejectedRecoveryWrites = 0;
        THashMap<ui32, TString> lastProposalStatuses;
        THashMap<ui32, TString> lastProposals;
        THashMap<ui32, TString> lastGathers;
        ui32 abortedFreshGathers = 0;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasProposeStorageConfig()) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                const auto& config = record.GetProposeStorageConfig().GetConfig();
                lastProposals[nodeId] = TStringBuilder() << "generation=" << config.GetGeneration()
                    << " matchesExpanded=" << (config.GetFingerprint() == expanded.GetFingerprint())
                    << " targeted=" << record.GetTargeted() << " timeoutUs=" << record.GetTimeoutUs()
                    << " clock=" << runtime.GetClock();
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigGather>()->Record;
                lastGathers[ev->Sender.NodeId()] = TStringBuilder() << "clock=" << runtime.GetClock()
                    << " recipient=" << nodeId << " cookie=" << record.GetCookie()
                    << " aborted=" << record.GetAborted() << " hasPropose=" << record.HasProposeStorageConfig()
                    << " statuses=" << record.GetProposeStorageConfig().StatusSize();
                abortedFreshGathers += ev->Sender.NodeId() >= 4 && nodeId == 1 && record.GetAborted();
                for (const auto& status : record.GetProposeStorageConfig().GetStatus()) {
                    lastProposalStatuses[status.GetNodeId().GetNodeId()] = status.ShortDebugString();
                }
            }
            if (HandleReplaceEvent(runtime, nodeId, *ev, edge, response)) {
                if (response) {
                    rejectRecoveryWrites = false;
                }
                return false;
            }
            if (rejectRecoveryWrites && nodeId >= 4
                && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetCommittedStorageConfig().GetGeneration() == 3) {
                ++rejectedRecoveryWrites;
                runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                    new NStorage::TEvNodeWardenWriteMetadataResult(std::nullopt, NPDisk::EPDiskMetadataOutcome::ERROR),
                    0, ev->Cookie), nodeId);
                return false;
            }
            if (nodeId >= 4 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Sender.NodeId() == 1 && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetRejected()) {
                rejectedBinding = true;
            }
            return true;
        };
        for (ui32 nodeId : {1, 2, 3}) {
            runtime.StopNode(nodeId);
            fixture.Metadata(nodeId).MutableProposedStorageConfig()->CopyFrom(expanded);
        }
        auto base = expanded;
        base.SetGeneration(0);
        base.ClearPrevConfig();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&base);
        for (ui32 nodeId : {4, 5, 6}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(expanded);
            fixture.Keepers.push_back(RegisterKeeper(runtime, base, nodeId, fixture.State));
        }
        for (ui32 nodeId : {1, 2, 3}) {
            runtime.StartNode(nodeId);
            fixture.Keepers[nodeId - 1] = RegisterKeeper(runtime, fixture.Config, nodeId, fixture.State);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return rejectedBinding; }));
        for (ui32 nodeId : {1, 2, 3}) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().GetGeneration(), 2);
        }

        edge = runtime.AllocateEdgeActor(1);
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        for (ui32 attempt = 0; attempt != 3; ++attempt) {
            response.reset();
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableReplaceStorageConfig()->SetYAML(fixture.MakeReplacementYaml(expanded, 0, "mirror-3-dc"));
            request->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(true);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(45)),
                          "repeated Replace did not finish");
            if (response->GetStatus() == TResult::OK) {
                break;
            }
            UNIT_ASSERT_C(response->GetStatus() == TResult::RACE || response->GetStatus() == TResult::NO_QUORUM
                || (response->GetStatus() == TResult::ERROR && TString(response->GetErrorReason()).Contains(
                    "no quorum for ProposedStorageConfig")), response->DebugString());
            if (!attempt) {
                SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(100));
                for (ui32 nodeId : {4, 5, 6}) {
                    runtime.ReconnectNodes(1, nodeId);
                }
            }
            const bool converged = SimUntil(runtime, [&] {
                return FindConvergedRoot(runtime, fixture.Keepers) == 1;
            }, TDuration::Seconds(90));
            TStringBuilder state;
            if (!converged) {
                state << " response=" << response->ShortDebugString()
                      << " rejectedRecoveryWrites=" << rejectedRecoveryWrites
                      << " abortedFreshGathers=" << abortedFreshGathers;
                for (const TActorId keeperId : fixture.Keepers) {
                    const auto& metadata = fixture.Metadata(keeperId.NodeId());
                    state << " [node=" << keeperId.NodeId()
                          << " diskCommitted=" << metadata.GetCommittedStorageConfig().GetGeneration()
                          << " diskProposed=" << metadata.GetProposedStorageConfig().GetGeneration()
                          << " committedMatchesExpanded=" << (metadata.GetCommittedStorageConfig().GetFingerprint() == expanded.GetFingerprint())
                          << " proposedMatchesExpanded=" << (metadata.GetProposedStorageConfig().GetFingerprint() == expanded.GetFingerprint());
                    if (const auto it = fixture.State.NodeWardenConfigs.find(keeperId.NodeId());
                        it != fixture.State.NodeWardenConfigs.end() && it->second) {
                        state << " applied=" << it->second->GetGeneration()
                              << " matchesExpanded=" << (it->second->GetFingerprint() == expanded.GetFingerprint());
                    }
                    const bool found = runtime.WrapInActorContext(keeperId, [&](IActor *actor) {
                        const auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
                        state << " root=" << keeper->GetRootNodeId()
                              << " nodeQuorum=" << keeper->PartOfNodeQuorum();
                    });
                    state << " alive=" << found;
                    if (found) {
                        state << " hasScepter=" << QueryKeeper(runtime, keeperId, true).HasScepter();
                    }
                    if (const auto it = lastProposals.find(keeperId.NodeId()); it != lastProposals.end()) {
                        state << " lastProposed=" << it->second;
                    }
                    if (const auto it = lastProposalStatuses.find(keeperId.NodeId()); it != lastProposalStatuses.end()) {
                        state << " lastProposal=" << it->second;
                    }
                    if (const auto it = lastGathers.find(keeperId.NodeId()); it != lastGathers.end()) {
                        state << " lastGather=" << it->second;
                    }
                    state << ']';
                }
            }
            UNIT_ASSERT_C(converged, "expanded nodes did not bind after configuration recovery:" << state);
        }
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        UNIT_ASSERT(rejectedRecoveryWrites);
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT(winner.GetGeneration() >= 4);
        UNIT_ASSERT_VALUES_EQUAL(winner.AllNodesSize(), 6);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(AdmissionRejectsReplayAfterFormat) {
        CheckReplaceAddsFreshStorageNodesWithMirror3dc(EFreshStorageMembershipScenario::ReplayAfterFormat);
    }

    Y_UNIT_TEST(AdmissionIgnoresReplyAfterFormat) {
        CheckReplaceAddsFreshStorageNodesWithMirror3dc(EFreshStorageMembershipScenario::AdmissionReplyAfterFormat);
    }

    Y_UNIT_TEST(AdmissionAbortsOnRootLoss) {
        CheckReplaceAddsFreshStorageNodesWithMirror3dc(EFreshStorageMembershipScenario::RootLossDuringAdmission);
    }

    enum class EPartialReplaceScenario {
        NewerReplace,
        FormatReplica,
        ConcurrentReplace,
        NoCommittedQuorum,
    };

    void CheckReplaceAfterPartialDelivery(EPartialReplaceScenario scenario) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& runtime = fixture.Runtime;
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        const TActorId edge = runtime.AllocateEdgeActor(1);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> competingResponse;
        TActorId competingEdge;
        std::vector<std::unique_ptr<IEventHandle>> delayedPublications;
        std::vector<std::unique_ptr<IEventHandle>> delayedCommits;
        bool delayPublication = true;
        bool delayCommit = scenario == EPartialReplaceScenario::NoCommittedQuorum;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, edge, response)) {
                return false;
            }
            if (competingEdge && CaptureInvokeResult(*ev, competingEdge, competingResponse)) {
                return false;
            }
            if (delayPublication && nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetCommittedStorageConfig().GetGeneration() == 2) {
                delayedPublications.push_back(std::move(ev));
                return false;
            }
            if (delayCommit && nodeId != 1
                && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetCommittedStorageConfig().GetGeneration() == 2) {
                delayedCommits.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        const auto sendReplace = [&](ui64 version, TActorId sender, const TString& suffix = {}) {
            auto request = fixture.MakeReplaceRequest(version, "none", suffix);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), sender, request.release()), 1);
        };
        sendReplace(0, edge);
        competingEdge = scenario == EPartialReplaceScenario::ConcurrentReplace ? runtime.AllocateEdgeActor(1) : TActorId();
        if (competingEdge) {
            sendReplace(0, competingEdge, "# competing body\n");
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)),
                      "replace did not confirm its committed quorum");
        const auto expectedStatus = scenario == EPartialReplaceScenario::NoCommittedQuorum
                                    ? NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM
                                    : NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK;
        UNIT_ASSERT_C(response->GetStatus() == expectedStatus, response->DebugString());
        UNIT_ASSERT(!delayedPublications.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(),
                                 scenario == EPartialReplaceScenario::NoCommittedQuorum ? 1 : 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);
        auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();

        if (competingEdge) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return competingResponse.has_value(); }));
            UNIT_ASSERT_C(competingResponse->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
                          competingResponse->DebugString());
            UNIT_ASSERT(TString(competingResponse->GetErrorReason()).Contains("version must be increasing by one"));
        }
        if (scenario == EPartialReplaceScenario::NewerReplace || competingEdge) {
            const TActorId nextEdge = runtime.AllocateEdgeActor(1);
            sendReplace(1, nextEdge);
            auto result = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(nextEdge);
            UNIT_ASSERT_C(result->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                          result->Get()->Record.DebugString());
            winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 3);
        } else if (scenario == EPartialReplaceScenario::FormatReplica) {
            fixture.FormatKeeper(2);
        }

        delayPublication = false;
        delayCommit = false;
        ReplayDelayedEvents(runtime, delayedPublications);
        ReplayDelayedEvents(runtime, delayedCommits);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(ReplaceFencesLatePublication) {
        CheckReplaceAfterPartialDelivery(EPartialReplaceScenario::NewerReplace);
    }

    Y_UNIT_TEST(ReplaceSurvivesReplicaFormat) {
        CheckReplaceAfterPartialDelivery(EPartialReplaceScenario::FormatReplica);
    }

    Y_UNIT_TEST(ConcurrentReplaceHasOneWinner) {
        CheckReplaceAfterPartialDelivery(EPartialReplaceScenario::ConcurrentReplace);
    }

    Y_UNIT_TEST(FailedReplaceConvergesAfterLateWrites) {
        CheckReplaceAfterPartialDelivery(EPartialReplaceScenario::NoCommittedQuorum);
    }

    Y_UNIT_TEST(ReplaceSurvivesChainCutAndLateReply) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 2}};
        auto& runtime = fixture.Runtime;
        THashMap<TActorId, ui32> peerByProxy;
        bool partitioned = false;
        bool dropPublication = true;
        ui32 droppedPublications = 0;
        bool holdFreshReply = true;
        bool sawForwardedReplace = false;
        bool sawForwardedQuery = false;
        TActorId replaceEdge;
        TActorId queryEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> replaceResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> queryResponse;
        std::vector<std::unique_ptr<IEventHandle>> delayedReplies;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (partitioned && FailSubscriptionAcrossCut(runtime, nodeId, *ev, peerByProxy)) {
                return false;
            }
            if (ReplyToReplaceController(runtime, nodeId, *ev, replaceEdge)) {
                return false;
            }
            if ((replaceEdge && CaptureInvokeResult(*ev, replaceEdge, replaceResponse))
                || (queryEdge && CaptureInvokeResult(*ev, queryEdge, queryResponse))) {
                return false;
            }
            if (dropPublication && nodeId == 3
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetCommittedStorageConfig().GetGeneration() == 2) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), 2);
                ++droppedPublications;
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && nodeId == 1 && ev->Sender.NodeId() == 2) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                sawForwardedReplace |= record.HasReplaceStorageConfig();
                sawForwardedQuery |= record.GetQueryConfig().GetRequireFresh();
            }
            if (holdFreshReply && nodeId == 2 && ev->Sender.NodeId() == 1
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (record.HasQueryConfig() && record.GetQueryConfig().GetFresh()) {
                    delayedReplies.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        fixture.Start();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        peerByProxy = fixture.InterconnectPeers();
        replaceEdge = runtime.AllocateEdgeActor(3);
        auto replace = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        runtime.Send(new IEventHandle(fixture.Keepers[2], replaceEdge, replace.release()), 3);
        UNIT_ASSERT(SimUntil(runtime, [&] { return replaceResponse.has_value(); }, TDuration::Seconds(35)));
        UNIT_ASSERT_C(replaceResponse->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      replaceResponse->DebugString());
        UNIT_ASSERT(sawForwardedReplace);
        const auto winner = fixture.Metadata(1).GetCommittedStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetFingerprint(), winner.GetFingerprint());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT(droppedPublications);

        queryEdge = runtime.AllocateEdgeActor(3);
        auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        query->Record.MutableQueryConfig()->SetRequireFresh(true);
        runtime.Send(new IEventHandle(fixture.Keepers[2], queryEdge, query.release()), 3);
        UNIT_ASSERT(SimUntil(runtime, [&] { return !delayedReplies.empty(); }));
        UNIT_ASSERT(sawForwardedQuery);
        UNIT_ASSERT(!queryResponse);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);

        partitioned = true;
        fixture.State.RequiredParentNodeIds[2] = 0;
        dropPublication = false;
        fixture.Disconnect(1, 2);
        const std::vector<TActorId> survivingQuorum{fixture.Keepers[1], fixture.Keepers[2]};
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, survivingQuorum) == 2; }),
                      "B and C did not elect a root after the A-B connection failed");
        UNIT_ASSERT(!FindConvergedRoot(runtime, {fixture.Keepers[0]}));
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers[0]).HasScepter());

        partitioned = false;
        fixture.State.RequiredParentNodeIds[1] = 3;
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, fixture.Keepers) == 2; }));
        holdFreshReply = false;
        ReplayDelayedEvents(runtime, delayedReplies);
        CheckDurableConfigConverges(fixture, winner);
        CheckStaysConverged(runtime, fixture.Keepers);
        UNIT_ASSERT(queryResponse);
        UNIT_ASSERT_C(queryResponse->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE,
                      queryResponse->DebugString());
    }

    void CheckReplaceSameGenerationAfterFailover(bool stopRoot) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 2}};
        auto& runtime = fixture.Runtime;
        bool partitioned = false;
        THashMap<TActorId, ui32> peerByProxy;
        TActorId firstEdge;
        TActorId secondEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> firstResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> secondResponse;
        bool holdOldProposal = true;
        bool holdPreflight = false;
        std::vector<std::unique_ptr<IEventHandle>> oldProposals;
        std::vector<std::unique_ptr<IEventHandle>> preflightReplies;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (partitioned && FailSubscriptionAcrossCut(runtime, nodeId, *ev, peerByProxy)) {
                return false;
            }
            if (ReplyToReplaceController(runtime, nodeId, *ev, secondEdge)) {
                return false;
            }
            if ((firstEdge && CaptureInvokeResult(*ev, firstEdge, firstResponse))
                || (secondEdge && CaptureInvokeResult(*ev, secondEdge, secondResponse))) {
                return false;
            }
            if (holdOldProposal && nodeId != 1
                && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig().GetGeneration() == 2) {
                oldProposals.push_back(std::move(ev));
                return false;
            }
            if (holdPreflight && nodeId != 1
                && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadataResult::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeWardenReadMetadataResult>()->Record;
                if (record.GetCommittedStorageConfig().GetGeneration() == 1 && !record.HasProposedStorageConfig()) {
                    preflightReplies.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        fixture.Start();
        peerByProxy = fixture.InterconnectPeers();
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        firstEdge = runtime.AllocateEdgeActor(1);
        secondEdge = runtime.AllocateEdgeActor(2);
        const auto sendReplace = [&](TActorId keeper, TActorId sender, ui64 version, const TString& suffix = {}) {
            auto request = fixture.MakeReplaceRequest(version, "mirror-3-dc", suffix);
            runtime.Send(new IEventHandle(keeper, sender, request.release()), keeper.NodeId());
        };
        sendReplace(fixture.Keepers[0], firstEdge, 0);
        UNIT_ASSERT(SimUntil(runtime, [&] { return oldProposals.size() == 2 || firstResponse.has_value(); }));
        UNIT_ASSERT_C(!firstResponse.has_value(), (firstResponse ? firstResponse->DebugString() : ""));
        const auto firstConfig = oldProposals.front()->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(firstConfig.GetGeneration(), 2);

        fixture.State.RequiredParentNodeIds[2] = 0;
        if (stopRoot) {
            runtime.StopNode(1);
        } else {
            partitioned = true;
            fixture.Disconnect(1, 2);
        }
        const std::vector<TActorId> availableKeepers{fixture.Keepers[1], fixture.Keepers[2]};
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, availableKeepers) == 2; },
                             TDuration::Seconds(30), 2));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers[1]).GetQueryConfig().GetConfig().GetGeneration(), 1);
        holdPreflight = true;
        sendReplace(fixture.Keepers[1], secondEdge, 0, "# replacement after failover\n");
        UNIT_ASSERT(SimUntil(runtime, [&] { return preflightReplies.size() == 2 || secondResponse.has_value(); },
                             TDuration::Seconds(30), 2));
        UNIT_ASSERT_C(!secondResponse.has_value(), (secondResponse ? secondResponse->DebugString() : ""));

        holdOldProposal = false;
        ReplayDelayedEvents(runtime, oldProposals);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return fixture.Metadata(2).GetProposedStorageConfig().GetFingerprint() == firstConfig.GetFingerprint()
                   && fixture.Metadata(3).GetProposedStorageConfig().GetFingerprint() == firstConfig.GetFingerprint();
        }, TDuration::Seconds(1), 2));
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(1), 2);
        holdPreflight = false;
        ReplayDelayedEvents(runtime, preflightReplies);
        UNIT_ASSERT(SimUntil(runtime, [&] { return secondResponse.has_value(); }, TDuration::Seconds(35), 2));
        UNIT_ASSERT_C(secondResponse->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      secondResponse->DebugString());
        const auto secondConfig = QueryKeeper(runtime, fixture.Keepers[1]).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(secondConfig.GetGeneration(), 2);
        UNIT_ASSERT(secondConfig.GetFingerprint() != firstConfig.GetFingerprint());
        UNIT_ASSERT(!firstResponse.has_value() || firstResponse->GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);

        if (stopRoot) {
            runtime.StartNode(1);
            fixture.Keepers[0] = RegisterKeeper(runtime, fixture.Config, 1, fixture.State);
        } else {
            partitioned = false;
        }
        fixture.State.RequiredParentNodeIds[1] = 3;
        CheckDurableConfigConverges(fixture, secondConfig);
        UNIT_ASSERT(!firstResponse || firstResponse->GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        secondResponse.reset();
        sendReplace(fixture.Keepers[1], secondEdge, 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return secondResponse.has_value(); }));
        UNIT_ASSERT_C(secondResponse->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      secondResponse->DebugString());
        const auto finalConfig = QueryKeeper(runtime, fixture.Keepers[1]).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(finalConfig.GetGeneration(), 3);
        CheckDurableConfigConverges(fixture, finalConfig);
        UNIT_ASSERT(!firstResponse || firstResponse->GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
    }

    Y_UNIT_TEST(FailoverFencesCancelledProposal) {
        CheckReplaceSameGenerationAfterFailover(true);
    }

    Y_UNIT_TEST(ChainCutFencesCancelledProposal) {
        CheckReplaceSameGenerationAfterFailover(false);
    }

    void CheckNewReplaceAfterLegacyFork(bool withCommittedQuorum, ui32 rootNodeId = 2) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = rootNodeId;
        auto majority = MakeNextConfig(fixture.Config);
        auto minority = majority;
        minority.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&minority);
        fixture.Metadata(1).MutableCommittedStorageConfig()->CopyFrom(minority);
        for (ui32 nodeId : {2, 3}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(majority);
        }
        if (!withCommittedQuorum) {
            fixture.Metadata(3).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        auto& runtime = fixture.Runtime;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, edge, response)) {
                return false;
            }
            return true;
        };
        fixture.Start();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetFingerprint(),
                                 minority.GetFingerprint());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetFingerprint(),
                                 majority.GetFingerprint());
        edge = runtime.AllocateEdgeActor(rootNodeId);
        auto freshQuery = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        freshQuery->Record.MutableQueryConfig()->SetRequireFresh(true);
        runtime.Send(new IEventHandle(fixture.Keepers[rootNodeId - 1], edge, freshQuery.release()), rootNodeId);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)));
        UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM,
                      response->DebugString());
        response.reset();
        auto request = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        runtime.Send(new IEventHandle(fixture.Keepers[rootNodeId - 1], edge, request.release()), rootNodeId);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)));
        UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers[rootNodeId - 1]).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 3);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(ReplaceRepairsLegacyFork) {
        CheckNewReplaceAfterLegacyFork(true);
    }

    Y_UNIT_TEST(ReplaceRepairsForkWithoutQuorum) {
        CheckNewReplaceAfterLegacyFork(false);
    }

    Y_UNIT_TEST(MinorityRootRepairsLegacyFork) {
        CheckNewReplaceAfterLegacyFork(true, 1);
    }

    Y_UNIT_TEST(ReplaceFencesLateRefillReply) {
        TCommitConfirmationFixture fixture;
        fixture.Metadata(3).Clear();
        auto& runtime = fixture.Runtime;
        TActorId edge;
        bool delayReply = true;
        ui32 heldOrdinary = 0;
        std::vector<std::unique_ptr<IEventHandle>> delayedReplies;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, edge)) {
                return false;
            }
            if (delayReply && nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (record.HasQueryConfig() && record.GetQueryConfig().GetFresh()
                    && record.GetQueryConfig().GetConfig().GetGeneration() == 1) {
                    delayedReplies.push_back(std::move(ev));
                    return false;
                }
            }
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetCommittedStorageConfig().GetGeneration() == 2) {
                ++heldOrdinary;
            }
            return true;
        };
        fixture.Start(true);
        edge = runtime.AllocateEdgeActor(1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return !delayedReplies.empty(); }));
        UNIT_ASSERT(!fixture.Metadata(3).HasCommittedStorageConfig());

        auto request = fixture.MakeReplaceRequest();
        InvokeKeeper(runtime, fixture.Keepers.front(), std::move(request));
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        UNIT_ASSERT(SimUntil(runtime, [&] { return heldOrdinary != 0; }));
        UNIT_ASSERT(!fixture.Metadata(3).HasCommittedStorageConfig());

        delayReply = false;
        ReplayDelayedEvents(runtime, delayedReplies);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(RefillRetriesLostReply) {
        TCommitConfirmationFixture fixture;
        fixture.Metadata(3).Clear();
        ui32 freshReplies = 0;
        fixture.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (record.HasQueryConfig() && record.GetQueryConfig().GetFresh()) {
                    return ++freshReplies != 1;
                }
            }
            return true;
        };
        fixture.Start(true);
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return freshReplies != 0; }));
        UNIT_ASSERT(!fixture.Metadata(3).HasCommittedStorageConfig());
        UNIT_ASSERT(SimUntil(fixture.Runtime, [&] { return freshReplies >= 2; }, TDuration::Seconds(35)));
        CheckDurableConfigConverges(fixture, fixture.Config);
    }

    void CheckCommittedConfigDelivery(bool legacyReceiver) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& [config, state, runtime, keepers] = fixture;
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        auto updated = config;
        updated.SetGeneration(2);
        updated.MutablePrevConfig()->CopyFrom(config);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&updated);

        bool firstDeliveryDropped = false;
        ui32 repeatedDeliveries = 0;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (legacyReceiver && ev->GetTypeRewrite() == NStorage::TEvNodeConfigPush::EventType
                && ev->Sender.NodeId() == 3) {
                for (const auto& item : ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetBoundNodes()) {
                    if (item.GetNodeId().GetNodeId() == 3 && item.GetMeta().GetGeneration() == 2) {
                        return false;
                    }
                }
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType && nodeId == 3
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetCommittedStorageConfig().GetGeneration() == 2) {
                if (!firstDeliveryDropped) {
                    firstDeliveryDropped = true;
                    return false;
                }
                ++repeatedDeliveries;
            }
            return true;
        };
        UNIT_ASSERT(runtime.WrapInActorContext(keepers.front(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyCommittedStorageConfig(updated);
        }));
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return firstDeliveryDropped && fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration() == 2;
        }, TDuration::MilliSeconds(500)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return fixture.Metadata(3).GetCommittedStorageConfig().GetFingerprint() == updated.GetFingerprint();
        }, TDuration::Seconds(5)), "lagging node did not receive a retry of the committed configuration");
        UNIT_ASSERT(repeatedDeliveries);
        const ui32 deliveriesAfterCatchup = repeatedDeliveries;
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(3));
        if (legacyReceiver) {
            UNIT_ASSERT(repeatedDeliveries > deliveriesAfterCatchup);
            UNIT_ASSERT(repeatedDeliveries <= deliveriesAfterCatchup + 4);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(repeatedDeliveries, deliveriesAfterCatchup);
        }
    }

    Y_UNIT_TEST(DeliveryRetriesLostPublication) {
        CheckCommittedConfigDelivery(false);
    }

    Y_UNIT_TEST(DeliveryRetriesWithoutMetadataFeedback) {
        CheckCommittedConfigDelivery(true);
    }

    void CheckPropagationSkipsDifferentConfig(bool higherGeneration) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& [config, state, runtime, keepers] = fixture;
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        auto updated = config;
        updated.SetGeneration(2);
        updated.MutablePrevConfig()->CopyFrom(config);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&updated);
        auto different = updated;
        different.SetSelfAssemblyUUID("other-config");
        if (higherGeneration) {
            different.SetGeneration(3);
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&different);

        ui32 blockedDeliveries = 0;
        bool sawDifferentMeta = false;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType && nodeId == 3
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetCommittedStorageConfig().GetFingerprint()
                   == updated.GetFingerprint()) {
                ++blockedDeliveries;
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigPush::EventType && nodeId == 1) {
                for (const auto& item : ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetBoundNodes()) {
                    sawDifferentMeta |= item.GetNodeId().GetNodeId() == 3
                                        && item.GetMeta().GetFingerprint() == different.GetFingerprint();
                }
            }
            return true;
        };
        UNIT_ASSERT(runtime.WrapInActorContext(keepers.front(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyCommittedStorageConfig(updated);
        }));
        UNIT_ASSERT(SimUntil(runtime, [&] { return blockedDeliveries != 0; }, TDuration::MilliSeconds(500)));
        UNIT_ASSERT(runtime.WrapInActorContext(keepers.back(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->ApplyCommittedStorageConfig(different);
        }));
        UNIT_ASSERT(SimUntil(runtime, [&] { return sawDifferentMeta; }, TDuration::MilliSeconds(500)));
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        const ui32 deliveriesBeforeRetry = blockedDeliveries;
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(4));
        UNIT_ASSERT_VALUES_EQUAL(blockedDeliveries, deliveriesBeforeRetry);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetFingerprint(), different.GetFingerprint());
    }

    Y_UNIT_TEST(DeliverySkipsConflict) {
        CheckPropagationSkipsDifferentConfig(false);
    }

    Y_UNIT_TEST(DeliverySkipsNewerGeneration) {
        CheckPropagationSkipsDifferentConfig(true);
    }

    void CheckReplaceCommitConfirmation(ECommitConfirmationScenario scenario) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& [config, state, runtime, keepers] = fixture;

        // Memory already contains this YAML, but only one durable committed copy remains.
        for (ui32 nodeId : {2, 3}) {
            fixture.Metadata(nodeId).ClearCommittedStorageConfig();
            fixture.Metadata(nodeId).MutableProposedStorageConfig()->CopyFrom(config);
        }
        if (scenario == ECommitConfirmationScenario::ConflictingCommit
            || scenario == ECommitConfirmationScenario::NewerCommit) {
            auto conflicting = config;
            conflicting.SetSelfAssemblyUUID("conflicting-commit");
            if (scenario == ECommitConfirmationScenario::NewerCommit) {
                conflicting.SetGeneration(config.GetGeneration() + 1);
            }
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&conflicting);
            fixture.Metadata(2).MutableCommittedStorageConfig()->CopyFrom(conflicting);
        }

        const TActorId edge = runtime.AllocateEdgeActor(1);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        bool stallReads = scenario == ECommitConfirmationScenario::StalledRead;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            return !stallReads || ev->GetTypeRewrite() != NStorage::TEvNodeWardenReadMetadata::EventType;
        };
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableReplaceStorageConfig()->SetDryRun(scenario == ECommitConfirmationScenario::DryRun);
        request->Record.MutableReplaceStorageConfig()->SetYAML("");
        runtime.Send(new IEventHandle(keepers.front(), edge, request.release()), 1);

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        TResult::EStatus expectedStatus = TResult::OK;
        if (scenario == ECommitConfirmationScenario::ConflictingCommit) {
            expectedStatus = TResult::ERROR;
        } else if (scenario == ECommitConfirmationScenario::NewerCommit) {
            expectedStatus = TResult::RACE;
        } else if (scenario != ECommitConfirmationScenario::DryRun) {
            UNIT_ASSERT_C(!SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(2)),
                          "replace returned before committed quorum was available");
            if (scenario == ECommitConfirmationScenario::DelayedCommit) {
                // A quorum suffices even while node 3 still has only proposed metadata.
                fixture.Metadata(2).MutableCommittedStorageConfig()->CopyFrom(config);
            } else if (scenario == ECommitConfirmationScenario::RootChange) {
                UNIT_ASSERT(runtime.WrapInActorContext(keepers.front(), [&](IActor *actor) {
                    static_cast<NStorage::TDistributedConfigKeeper*>(actor)->SwitchToError("root changed during replace");
                }));
                expectedStatus = TResult::RACE;
            } else {
                expectedStatus = TResult::NO_QUORUM;
            }
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)),
                      "replace confirmation did not finish");
        UNIT_ASSERT_C(response->GetStatus() == expectedStatus, response->DebugString());
        if (expectedStatus == TResult::NO_QUORUM) {
            UNIT_ASSERT(TString(response->GetErrorReason()).Contains("may already be applied"));
        }
        if (scenario != ECommitConfirmationScenario::RootChange) {
            stallReads = false;
            UNIT_ASSERT(QueryKeeper(runtime, keepers.front()).HasScepter());
        }
    }

    Y_UNIT_TEST(RepeatedReplaceWaitsForCommittedQuorum) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::DelayedCommit);
    }

    Y_UNIT_TEST(CommitConfirmationWaitsForInlineRecovery) {
        TCommitConfirmationFixture fixture(true);
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 1}};
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        auto& runtime = fixture.Runtime;
        fixture.Disconnect(1, 3);
        runtime.StopNode(3);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        const std::vector<TActorId> survivors{fixture.Keepers[0], fixture.Keepers[1]};
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, survivors) == 1; }));
        auto accepted = MakeNextConfig(fixture.Config);
        accepted.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&accepted);
        fixture.Metadata(2).MutableProposedStorageConfig()->CopyFrom(accepted);
        UNIT_ASSERT(HasQuorum(fixture.Config, {1, 2}));
        UNIT_ASSERT(!HasQuorum(accepted, {2}));
        UNIT_ASSERT(HasQuorum(accepted, {2, 3}));
        const TActorId edge = runtime.AllocateEdgeActor(1);
        TActorId handlerId;
        bool holdRecovery = true;
        THashSet<ui64> collectCookies;
        THashSet<ui64> proposalCookies;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedWrites;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeConfigGather::EventType && !ev->InterconnectSession
                && ev->Sender == fixture.Keepers[0]
                && ev->Get<NStorage::TEvNodeConfigGather>()->Record.HasCollectConfigs()) {
                handlerId = ev->GetRecipientRewrite();
            }
            if (nodeId == 2 && ev->InterconnectSession && type == NStorage::TEvNodeConfigScatter::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (record.HasCollectConfigs()) {
                    collectCookies.insert(record.GetCookie());
                } else if (record.HasProposeStorageConfig()) {
                    proposalCookies.insert(record.GetCookie());
                }
            }
            if (holdRecovery && nodeId == 2 && type == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig().GetGeneration() == 2) {
                delayedWrites.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableReplaceStorageConfig()->SetYAML("");
        runtime.Send(new IEventHandle(fixture.Keepers[0], edge, request.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return !delayedWrites.empty() || response; }, TDuration::Seconds(25)),
            "commit confirmation did not start inline proposal recovery");
        UNIT_ASSERT_C(!response, (response ? response->DebugString() : TString()));
        UNIT_ASSERT(!delayedWrites.empty());
        UNIT_ASSERT(handlerId && runtime.GetActor(handlerId));
        const size_t collectionsBeforeWakeup = collectCookies.size();
        UNIT_ASSERT(collectionsBeforeWakeup >= 2);
        UNIT_ASSERT_VALUES_EQUAL(proposalCookies.size(), 1);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(3));
        UNIT_ASSERT(!response);
        UNIT_ASSERT(runtime.GetActor(handlerId));
        UNIT_ASSERT_VALUES_EQUAL(collectCookies.size(), collectionsBeforeWakeup);
        UNIT_ASSERT_VALUES_EQUAL(proposalCookies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(), 1);
        holdRecovery = false;
        ReplayDelayedEvents(runtime, delayedWrites);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(10)));
        UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE,
            response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers[0]).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.SerializeAsString(), accepted.SerializeAsString());
        UNIT_ASSERT(winner.GetSelfManagementConfig().GetAutomaticBootstrap());
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return fixture.Metadata(1).GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint()
                && fixture.Metadata(2).GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint();
        }));
    }

    Y_UNIT_TEST(ReplaceWithoutQuorumUnblocksQueue) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::Timeout);
    }

    Y_UNIT_TEST(CommitConfirmationReadTimeout) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::StalledRead);
    }

    Y_UNIT_TEST(ReplaceRejectsConflictingCommit) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::ConflictingCommit);
    }

    Y_UNIT_TEST(RepeatedReplaceRejectsNewerCommittedConfig) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::NewerCommit);
    }

    Y_UNIT_TEST(CommitConfirmationAbortsOnRootChange) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::RootChange);
    }

    Y_UNIT_TEST(ReplaceDryRunDoesNotWaitForCommittedQuorum) {
        CheckReplaceCommitConfirmation(ECommitConfirmationScenario::DryRun);
    }

    enum class ESlowMetadataOperation {
        Read,
        ProposedWrite,
        Gather,
    };

    void CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation operation, ui32 slowNodeId,
                                             bool forceChain, bool hasQuorum) {
        UNIT_ASSERT(1 <= slowNodeId && slowNodeId <= 3);
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        if (forceChain) {
            fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 2}};
        }
        auto& runtime = fixture.Runtime;
        TActorId replaceEdge;
        TActorId queryEdge;
        bool holdMetadata = false;
        std::vector<std::unique_ptr<IEventHandle>> delayedMetadata;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> replaceResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> queryResponse;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, replaceEdge)) {
                return false;
            }
            if ((replaceEdge && CaptureInvokeResult(*ev, replaceEdge, replaceResponse))
                || (queryEdge && CaptureInvokeResult(*ev, queryEdge, queryResponse))) {
                return false;
            }
            const ui32 memberNodeId = operation == ESlowMetadataOperation::Gather ? ev->Sender.NodeId() : nodeId;
            if (holdMetadata && (hasQuorum ? memberNodeId == slowNodeId : memberNodeId != 1)
                && ((operation == ESlowMetadataOperation::Gather
                        && ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType
                        && ev->InterconnectSession)
                    || (operation == ESlowMetadataOperation::Read
                        && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType)
                    || (operation == ESlowMetadataOperation::ProposedWrite
                        && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                        && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record
                            .GetProposedStorageConfig().GetGeneration() == 2))) {
                delayedMetadata.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        if (forceChain) {
            fixture.StartWithBoundChild(1, 2);
        } else {
            fixture.Start();
        }
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        std::vector<ui32> healthyNodeIds{1, 2, 3};
        std::erase(healthyNodeIds, slowNodeId);
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, healthyNodeIds));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {healthyNodeIds.front()}));

        holdMetadata = true;
        replaceEdge = runtime.AllocateEdgeActor(1);
        auto replace = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, replace.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return !delayedMetadata.empty(); }));
        queryEdge = runtime.AllocateEdgeActor(1);
        auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        query->Record.MutableQueryConfig();
        runtime.Send(new IEventHandle(fixture.Keepers.front(), queryEdge, query.release()), 1);
        const TDuration operationTimeout = operation == ESlowMetadataOperation::Gather
            ? TDuration::Seconds(95) : TDuration::Seconds(45);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return replaceResponse && queryResponse; }, operationTimeout),
            "a slow metadata operation blocked replace or its queued query");
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(queryResponse->GetStatus() == TResult::OK, queryResponse->DebugString());
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        if (hasQuorum) {
            UNIT_ASSERT_C(replaceResponse->GetStatus() == TResult::OK, replaceResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(queryResponse->GetQueryConfig().GetConfig().GetGeneration(), 2);
            for (ui32 nodeId : healthyNodeIds) {
                UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().GetGeneration(), 2);
            }
            replaceResponse.reset();
            auto next = fixture.MakeReplaceRequest(1, "mirror-3-dc");
            runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, next.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return replaceResponse.has_value(); }, operationTimeout),
                "a retired metadata operation blocked the next replace");
            UNIT_ASSERT_C(replaceResponse->GetStatus() == TResult::OK, replaceResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig()
                .GetConfig().GetGeneration(), 3);
        } else {
            UNIT_ASSERT_C(replaceResponse->GetStatus() != TResult::OK, replaceResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(queryResponse->GetQueryConfig().GetConfig().GetGeneration(), 1);
            for (ui32 nodeId : {1, 2, 3}) {
                UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().GetGeneration(), 1);
            }
        }

        holdMetadata = false;
        ReplayDelayedEvents(runtime, delayedMetadata);
        auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        CheckDurableConfigConverges(fixture, winner);
        const auto advanceGeneration = [&] {
            replaceResponse.reset();
            auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            advance->Record.MutableAdvanceGeneration();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, advance.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return replaceResponse.has_value(); }, TDuration::Seconds(45)),
                "late metadata completion blocked the next configuration update");
        };
        advanceGeneration();
        if (replaceResponse->GetStatus() == TResult::RACE) {
            UNIT_ASSERT(!hasQuorum && operation == ESlowMetadataOperation::ProposedWrite);
            UNIT_ASSERT_C(TString(replaceResponse->GetErrorReason()).Contains("Configuration was recovered"),
                replaceResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 1);
            winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
            CheckDurableConfigConverges(fixture, winner);
            advanceGeneration();
        }
        UNIT_ASSERT_C(replaceResponse->GetStatus() == TResult::OK, replaceResponse->DebugString());
        const auto advanced = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT(advanced.GetGeneration() > winner.GetGeneration());
        CheckDurableConfigConverges(fixture, advanced);
    }

    Y_UNIT_TEST(ReplaceSurvivesSlowLeafRead) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::Read, 3, false, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesSlowRootRead) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::Read, 1, false, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesSlowRootWrite) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::ProposedWrite, 1, false, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesSlowLeafWrite) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::ProposedWrite, 3, false, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesSlowIntermediateRead) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::Read, 2, true, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesSlowIntermediateWrite) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::ProposedWrite, 2, true, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesSilentLeaf) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::Gather, 3, false, true);
    }

    Y_UNIT_TEST(ReplaceSurvivesIndirectSilentLeaf) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::Gather, 3, true, true);
    }

    Y_UNIT_TEST(ReplaceReadFailureUnblocksQueue) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::Read, 3, false, false);
    }

    Y_UNIT_TEST(ReplaceWriteFailureUnblocksQueue) {
        CheckChangedReplaceWithSlowMetadata(ESlowMetadataOperation::ProposedWrite, 3, false, false);
    }

    void CheckRepeatedEmptyRebind(bool duringProposal) {
        TCommitConfirmationFixture fixture(false, 4);
        fixture.Config = MakeStorageConfig(4, 1, {1});
        fixture.Config.MutableSelfManagementConfig()->SetEnabled(true);
        for (auto& node : *fixture.Config.MutableAllNodes()) {
            node.MutableLocation()->SetDataCenter("dc-1");
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3, 4}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        fixture.Start(true);
        CheckDurableConfigConverges(fixture, fixture.Config);
        auto& runtime = fixture.Runtime;
        const TActorId edge = runtime.AllocateEdgeActor(1);
        ui32 emptyBinds = 0;
        ui32 delayedOperations = 0;
        THashSet<const IEventHandle*> delayed;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, edge)
                || CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (nodeId == 1 && ev->Sender.NodeId() == 4
                && type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.GetQueryConfig().GetRequireFresh()) {
                return false;
            }
            if (nodeId == 1 && ev->Sender.NodeId() == 4 && type == NStorage::TEvNodeConfigPush::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigPush>()->Record;
                if (record.GetInitial()) {
                    for (const auto& node : record.GetBoundNodes()) {
                        emptyBinds += node.GetNodeId().GetNodeId() == 4 && !node.GetMeta().GetGeneration();
                    }
                }
            }
            const bool slowOperation = duringProposal
                ? nodeId <= 3 && type == NStorage::TEvNodeWardenWriteMetadata::EventType
                    && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig().GetGeneration() == 2
                : nodeId == 1 && type == NStorage::TEvNodeWardenReadMetadata::EventType;
            if (slowOperation && !delayed.erase(ev.get())) {
                delayed.insert(ev.get());
                ++delayedOperations;
                runtime.Schedule(runtime.GetClock() + TDuration::Seconds(duringProposal ? 6 : 8), ev.release(), nullptr, nodeId);
                return false;
            }
            return true;
        };
        if (duringProposal) {
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, fixture.MakeReplaceRequest().release()), 1);
            UNIT_ASSERT(SimUntil(runtime, [&] { return delayedOperations >= 3; }));
        }
        fixture.FormatKeeper(4);
        if (!duringProposal) {
            auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            query->Record.MutableQueryConfig()->SetRequireFresh(true);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, query.release()), 1);
        }
        for (ui32 cycle = 0; cycle != 4; ++cycle) {
            SimUntil(runtime, [] { return false; }, TDuration::Seconds(5));
            fixture.FormatKeeper(4);
        }
        UNIT_ASSERT_C(emptyBinds >= 3, "the empty replica did not repeatedly bind");
        UNIT_ASSERT(delayedOperations);
        UNIT_ASSERT_C(response && response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            (response ? response->DebugString() : "a healthy quorum never completed the operation"));
        if (!duringProposal) {
            UNIT_ASSERT(response->GetQueryConfig().GetFresh());
            UNIT_ASSERT_VALUES_EQUAL(response->GetQueryConfig().GetConfig().GetFingerprint(), fixture.Config.GetFingerprint());
        } else {
            for (ui32 nodeId : {1, 2, 3}) {
                UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().GetGeneration(), 2);
            }
        }
    }

    Y_UNIT_TEST(RecoverySurvivesReplicaChurn) {
        CheckRepeatedEmptyRebind(false);
    }

    Y_UNIT_TEST(ProposalSurvivesReplicaChurn) {
        CheckRepeatedEmptyRebind(true);
    }

    void UseThreeMetadataDrives(TCommitConfirmationFixture& fixture) {
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        for (ui32 nodeId : {1, 2, 3}) {
            auto *host = fixture.Config.MutableBlobStorageConfig()->MutableDefineHostConfig(nodeId - 1);
            for (ui32 diskId : {2, 3}) {
                auto *drive = host->AddDrive();
                drive->SetPath("/dev/disk" + std::to_string(nodeId) + "_" + std::to_string(diskId));
                drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
            }
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3}) {
            for (ui32 diskId : {1, 2, 3}) {
                fixture.State.Metadata[std::pair<ui32, TString>{
                    nodeId, "/dev/disk" + std::to_string(nodeId) + "_" + std::to_string(diskId)}]
                    .MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
            }
        }
    }

    enum class ESlowDriveScenario {
        DelayedOnEveryNode,
        StalledOnEveryNode,
        StalledOnOneNode,
    };

    void CheckSlowMetadataDrive(ESlowDriveScenario scenario) {
        const bool queueReadsBehindWrite = scenario == ESlowDriveScenario::StalledOnEveryNode;
        TCommitConfirmationFixture fixture(true);
        UseThreeMetadataDrives(fixture);
        auto& runtime = fixture.Runtime;
        TActorId edge;
        bool holdWrites = false;
        bool holdNewProposal = false;
        std::vector<std::unique_ptr<IEventHandle>> held;
        std::vector<std::unique_ptr<IEventHandle>> heldNewProposal;
        THashSet<std::pair<ui32, TString>> delayedPaths;
        THashSet<const IEventHandle*> delayedEvents;
        THashSet<std::pair<TActorId, ui64>> slowPhysicalRequests;
        THashSet<std::pair<ui32, TString>> blockedPaths;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, edge)
                || (edge && CaptureInvokeResult(*ev, edge, response))) {
                return false;
            }
            if (queueReadsBehindWrite && holdWrites
                && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType
                && blockedPaths.contains(std::make_pair(nodeId, ev->Get<NStorage::TEvNodeWardenReadMetadata>()->Path))) {
                held.push_back(std::move(ev));
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                const auto& msg = *ev->Get<NStorage::TEvNodeWardenWriteMetadata>();
                if (holdNewProposal && !msg.Path.EndsWith("_3")
                    && msg.Record.GetProposedStorageConfig().GetGeneration() == 4) {
                    heldNewProposal.push_back(std::move(ev));
                    return false;
                }
                const bool slowNode = scenario != ESlowDriveScenario::StalledOnOneNode || nodeId == 3;
                if (slowNode && msg.Path.EndsWith("_3") && msg.Record.GetProposedStorageConfig().GetGeneration() >= 2) {
                    slowPhysicalRequests.emplace(ev->Sender, ev->Cookie);
                    if (holdWrites && scenario != ESlowDriveScenario::DelayedOnEveryNode) {
                        blockedPaths.emplace(nodeId, msg.Path);
                        held.push_back(std::move(ev));
                        return false;
                    }
                    if (holdWrites && !delayedEvents.erase(ev.get()) && delayedPaths.emplace(nodeId, msg.Path).second) {
                        delayedEvents.insert(ev.get());
                        runtime.Schedule(runtime.GetClock() + TDuration::Seconds(12), ev.release(), nullptr, nodeId);
                        return false;
                    }
                }
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        holdWrites = true;
        edge = runtime.AllocateEdgeActor(1);
        const ui32 replacements = scenario == ESlowDriveScenario::StalledOnOneNode ? 12
            : scenario == ESlowDriveScenario::StalledOnEveryNode ? 2 : 1;
        for (ui32 index = 0; index != replacements; ++index) {
            response.reset();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge,
                fixture.MakeReplaceRequest(index, "mirror-3-dc").release()), 1);
            const auto timeout = scenario == ESlowDriveScenario::DelayedOnEveryNode
                ? TDuration::MilliSeconds(11'500) : TDuration::Seconds(queueReadsBehindWrite ? 45 : 35);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, timeout),
                "the healthy disks did not complete the replacement");
            UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                response->DebugString());
        }
        if (scenario == ESlowDriveScenario::StalledOnEveryNode) {
            holdNewProposal = true;
            response.reset();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge,
                fixture.MakeReplaceRequest(2, "mirror-3-dc").release()), 1);
            UNIT_ASSERT(SimUntil(runtime, [&] { return heldNewProposal.size() == 6; }));
            holdWrites = false;
            ReplayDelayedEvents(runtime, held);
            SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
            UNIT_ASSERT(!response);
            holdNewProposal = false;
            ReplayDelayedEvents(runtime, heldNewProposal);
            UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }));
            UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                response->DebugString());
        }
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), replacements + 1
            + (scenario == ESlowDriveScenario::StalledOnEveryNode));
        holdWrites = false;
        ReplayDelayedEvents(runtime, held);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(fixture.State.Metadata, [&](const auto& item) {
                return item.second.GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint();
            });
        }, TDuration::Seconds(30)), "the final configuration did not reach the previously stalled drives");
        if (scenario == ESlowDriveScenario::StalledOnOneNode) {
            UNIT_ASSERT_C(slowPhysicalRequests.size() <= 4,
                "the stalled drive retained intermediate generations: " << slowPhysicalRequests.size());
        }
    }

    Y_UNIT_TEST(ReplaceCountsFastDisks) {
        CheckSlowMetadataDrive(ESlowDriveScenario::DelayedOnEveryNode);
    }

    Y_UNIT_TEST(StalledDriveCoalescesCommits) {
        CheckSlowMetadataDrive(ESlowDriveScenario::StalledOnOneNode);
    }

    Y_UNIT_TEST(ReplaceBypassesStalledDrive) {
        CheckSlowMetadataDrive(ESlowDriveScenario::StalledOnEveryNode);
    }

    void CheckChangedReplaceWithPausedIntermediate(bool transient) {
        constexpr ui32 middleNodeId = 7;
        TCommitConfirmationFixture fixture(true, 8);
        fixture.UseMirror3dcGroup(8);
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 7}, {3, 7}, {4, 7},
                                              {5, 7}, {6, 7}, {7, 1}, {8, 1}};
        auto& runtime = fixture.Runtime;
        bool pauseMiddle = false;
        bool observeRebinding = false;
        ui32 bypassAttempts = 0;
        ui32 capturedResponses = 0;
        TActorId replaceEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedEvents;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, replaceEdge)) {
                return false;
            }
            if (replaceEdge && CaptureInvokeResult(*ev, replaceEdge, response)) {
                ++capturedResponses;
                return false;
            }
            if (observeRebinding && ev->InterconnectSession
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigPush::EventType
                && ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetInitial()
                && nodeId != middleNodeId && 2 <= ev->Sender.NodeId() && ev->Sender.NodeId() <= 6) {
                ++bypassAttempts;
            }
            if (pauseMiddle && ev->GetRecipientRewrite() == fixture.Keepers[middleNodeId - 1]) {
                delayedEvents.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.StartWithBoundChild(1, middleNodeId);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);

        std::vector<TActorId> healthyKeepers;
        std::vector<ui32> healthyNodeIds;
        for (const auto keeper : fixture.Keepers) {
            if (keeper.NodeId() != middleNodeId) {
                healthyKeepers.push_back(keeper);
                healthyNodeIds.push_back(keeper.NodeId());
            }
        }
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, healthyNodeIds));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {1, 8}));

        fixture.State.RequiredParentNodeIds.clear();
        pauseMiddle = true;
        observeRebinding = true;
        const TInstant deadline = runtime.GetClock() + TDuration::Seconds(180);
        ui32 attempts = 0;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        do {
            ++attempts;
            response.reset();
            replaceEdge = runtime.AllocateEdgeActor(1);
            const ui64 generation = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig()
                .GetConfig().GetGeneration();
            auto request = fixture.MakeReplaceRequest(generation - 1, "mirror-3-dc");
            runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, request.release()), 1);
            if (transient && attempts == 1) {
                SimUntil(runtime, [] { return false; }, TDuration::Seconds(5));
                UNIT_ASSERT(!delayedEvents.empty());
                pauseMiddle = false;
                ReplayDelayedEvents(runtime, delayedEvents);
            }
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(95)),
                "a paused distconf intermediary blocked the request queue");
            UNIT_ASSERT_C(capturedResponses == attempts, "an invocation returned more than one result");
        } while (response->GetStatus() != TResult::OK && runtime.GetClock() < deadline);
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK,
            "a healthy disk quorum remained behind the paused intermediary after " << attempts
            << " attempts: " << response->DebugString());
        UNIT_ASSERT(FindConvergedRoot(runtime, healthyKeepers) == 1);
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT(winner.GetGeneration() >= 2);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(healthyKeepers, [&](TActorId keeper) {
                return fixture.Metadata(keeper.NodeId()).GetCommittedStorageConfig().GetFingerprint()
                    == winner.GetFingerprint();
            });
        }), "the successful replace did not reach the healthy disks");
        for (const auto keeper : healthyKeepers) {
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, keeper, true).HasScepter(), keeper.NodeId() == 1);
        }
        if (transient) {
            UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
            UNIT_ASSERT_VALUES_EQUAL(bypassAttempts, 0);
        } else {
            UNIT_ASSERT(!delayedEvents.empty());
            UNIT_ASSERT_C(bypassAttempts, "the healthy subtree never bypassed its silent parent");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(middleNodeId).GetCommittedStorageConfig().GetGeneration(), 1);
            pauseMiddle = false;
            ReplayDelayedEvents(runtime, delayedEvents);
        }
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == 1);
        response.reset();
        replaceEdge = runtime.AllocateEdgeActor(1);
        auto next = fixture.MakeReplaceRequest(winner.GetGeneration() - 1, "mirror-3-dc");
        runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, next.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(90)));
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto advanced = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(advanced.GetGeneration(), winner.GetGeneration() + 1);
        CheckDurableConfigConverges(fixture, advanced);
    }

    Y_UNIT_TEST(ReplaceBypassesPausedIntermediate) {
        CheckChangedReplaceWithPausedIntermediate(false);
    }

    Y_UNIT_TEST(BindingSurvivesBriefPause) {
        CheckChangedReplaceWithPausedIntermediate(true);
    }

    Y_UNIT_TEST(ReplaceSurvivesPausedRoot) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        bool pauseRoot = false;
        TActorId replaceEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedEvents;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, replaceEdge, response)) {
                return false;
            }
            if (pauseRoot && ev->GetRecipientRewrite() == fixture.Keepers.front()) {
                delayedEvents.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        const std::vector<TActorId> healthyKeepers{fixture.Keepers[1], fixture.Keepers[2]};
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {2, 3}));
        fixture.State.RequiredRootNodeId = 0;
        pauseRoot = true;
        std::optional<ui32> newRoot;
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            newRoot = FindConvergedRoot(runtime, healthyKeepers);
            return newRoot && *newRoot != 1;
        }, TDuration::Seconds(180)), "a paused root keeper prevented the healthy quorum from electing a root");
        const TActorId root = fixture.Keepers[*newRoot - 1];
        UNIT_ASSERT(QueryKeeper(runtime, root, true).HasScepter());
        replaceEdge = runtime.AllocateEdgeActor(root.NodeId());
        auto request = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        runtime.Send(new IEventHandle(root, replaceEdge, request.release()), root.NodeId());
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(95)),
            "the new root could not complete a replace while the old keeper remained paused");
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto winner = QueryKeeper(runtime, root).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        for (const auto keeper : healthyKeepers) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(keeper.NodeId()).GetCommittedStorageConfig().GetFingerprint(),
                winner.GetFingerprint());
        }
        UNIT_ASSERT(!delayedEvents.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration(), 1);
        pauseRoot = false;
        ReplayDelayedEvents(runtime, delayedEvents);
        runtime.ReconnectNodes(1, 2);
        runtime.ReconnectNodes(1, 3);
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == newRoot);
        for (const auto keeper : fixture.Keepers) {
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, keeper, true).HasScepter(), keeper == root);
        }
        response.reset();
        auto next = fixture.MakeReplaceRequest(1, "mirror-3-dc");
        runtime.Send(new IEventHandle(root, replaceEdge, next.release()), root.NodeId());
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(95)));
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto advanced = QueryKeeper(runtime, root).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(advanced.GetGeneration(), 3);
        CheckDurableConfigConverges(fixture, advanced);
    }

    void CheckBindingLivenessPeerCapability(bool selfManagement, bool legacyParent, bool pauseParent = false,
                                           bool disabledMetadata = false) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        if (!selfManagement) {
            if (disabledMetadata) {
                auto disabled = MakeNextConfig(fixture.Config);
                disabled.MutableSelfManagementConfig()->SetEnabled(false);
                NStorage::TDistributedConfigKeeper::UpdateFingerprint(&disabled);
                for (ui32 nodeId : {1, 2, 3}) {
                    fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(disabled);
                }
            } else {
                fixture.Config.SetGeneration(0);
                fixture.Config.MutableSelfManagementConfig()->SetEnabled(false);
                NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
                fixture.State.Metadata.clear();
            }
        }
        auto& runtime = fixture.Runtime;
        ui32 heartbeatRequests = 0;
        ui32 sessionPoisons = 0;
        ui32 disconnections = 0;
        bool trackDisconnects = false;
        bool pauseRoot = false;
        TActorId parentSession;
        std::vector<std::unique_ptr<IEventHandle>> pausedEvents;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (pauseRoot && ev->GetRecipientRewrite() == fixture.Keepers.front()) {
                pausedEvents.push_back(std::move(ev));
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigReversePush::EventType
                && nodeId == 2 && ev->Sender.NodeId() == 1) {
                auto& record = ev->Get<NStorage::TEvNodeConfigReversePush>()->Record;
                if (!record.GetRejected()) {
                    parentSession = ev->InterconnectSession;
                    if (legacyParent) {
                        // An old binary has neither this capability field nor QueryWorkingRoot.
                        record.ClearSupportsDirectRootQueries();
                    } else {
                        UNIT_ASSERT(record.GetSupportsDirectRootQueries());
                    }
                }
            }
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Sender.NodeId() == 2 && ev->Sender != fixture.Keepers[1]
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasQueryWorkingRoot()) {
                ++heartbeatRequests;
                if (legacyParent) {
                    // Model the exact response produced by the pre-QueryWorkingRoot protocol.
                    auto reply = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
                    reply->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
                    reply->Record.SetErrorReason("Request field not set");
                    auto handle = std::make_unique<IEventHandle>(ev->Sender, fixture.Keepers.front(),
                        reply.release(), 0, ev->Cookie);
                    handle->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
                    runtime.Send(handle.release(), nodeId);
                    return false;
                }
            }
            if (parentSession && type == TEvents::TSystem::Poison
                && ev->GetRecipientRewrite() == parentSession) {
                ++sessionPoisons;
            }
            if (trackDisconnects && type == TEvInterconnect::TEvNodeDisconnected::EventType
                && ((nodeId == 1 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 2)
                    || (nodeId == 2 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 1))) {
                ++disconnections;
            }
            return true;
        };
        fixture.Start();
        UNIT_ASSERT(parentSession);
        UNIT_ASSERT(runtime.WrapInActorContext(fixture.Keepers[1], [&](IActor*) {
            UNIT_ASSERT(!AppData()->FeatureFlags.GetSwitchToConfigV2());
            UNIT_ASSERT(!AppData()->FeatureFlags.GetForceDistconfDisable());
        }));
        const auto scepter = QueryKeeper(runtime, fixture.Keepers.front(), true).GetScepter();
        const auto current = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(current.GetSelfManagementConfig().GetEnabled(), selfManagement);
        if (disabledMetadata) {
            UNIT_ASSERT(fixture.Config.GetSelfManagementConfig().GetEnabled());
            UNIT_ASSERT(current.GetGeneration() > fixture.Config.GetGeneration());
        }
        trackDisconnects = true;
        if (pauseParent) {
            pauseRoot = true;
            const auto requester = runtime.Register(new TIgnoreActor, 1);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableQueryWorkingRoot();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), requester, request.release()), 1);
        }
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(180));
        UNIT_ASSERT_VALUES_EQUAL(sessionPoisons, 0);
        UNIT_ASSERT_VALUES_EQUAL(disconnections, 0);
        if (legacyParent || !selfManagement) {
            UNIT_ASSERT_VALUES_EQUAL(heartbeatRequests, 0);
        } else {
            UNIT_ASSERT_C(heartbeatRequests >= 3, "capable parents must retain the liveness watchdog");
        }
        UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == 1);
        if (pauseParent) {
            UNIT_ASSERT(!pausedEvents.empty());
            pauseRoot = false;
            ReplayDelayedEvents(runtime, pausedEvents);
        }
        const auto after = QueryKeeper(runtime, fixture.Keepers.front(), true);
        UNIT_ASSERT(after.HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(after.GetScepter().GetId(), scepter.GetId());
    }

    Y_UNIT_TEST(BindingToLegacyParentWithDistconfDisabled) {
        CheckBindingLivenessPeerCapability(false, true);
    }

    Y_UNIT_TEST(BindingToLegacyParentWithDistconfEnabled) {
        CheckBindingLivenessPeerCapability(true, true);
    }

    Y_UNIT_TEST(BindingToCapableParentWithDistconfDisabled) {
        CheckBindingLivenessPeerCapability(false, false);
    }

    Y_UNIT_TEST(BindingToCapableParentWithDistconfEnabled) {
        CheckBindingLivenessPeerCapability(true, false);
    }

    Y_UNIT_TEST(PausedCapableParentWithDistconfDisabledKeepsInterconnectSession) {
        CheckBindingLivenessPeerCapability(false, false, true);
    }

    Y_UNIT_TEST(CommittedDisableWithEnabledBaseKeepsInterconnectSession) {
        CheckBindingLivenessPeerCapability(false, false, true, true);
    }

    void CheckBindingLivenessIgnoresRetiredEvents(bool rebind) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        TActorId oldActor;
        TActorId newActor;
        TActorId oldSession;
        TActorId newSession;
        bool captureOld = false;
        bool observePoison = false;
        bool parentAdvertisedDirectRootQueries = false;
        ui32 heartbeatRequests = 0;
        ui32 sessionPoisons = 0;
        TActorId replaceEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::unique_ptr<IEventHandle> oldReply;
        std::vector<std::unique_ptr<IEventHandle>> oldTimers;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, replaceEdge, response)) {
                return false;
            }
            if (ev->InterconnectSession && nodeId == 2
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigReversePush>()->Record;
                if (!record.GetRejected()) {
                    UNIT_ASSERT_C(record.GetSupportsDirectRootQueries(),
                        "the parent did not advertise direct root query support to its child");
                    parentAdvertisedDirectRootQueries = true;
                }
            }
            if (ev->InterconnectSession && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasQueryWorkingRoot()
                && ev->Sender.NodeId() == 2 && ev->Sender != fixture.Keepers[1]) {
                ++heartbeatRequests;
                if (captureOld && !oldActor) {
                    oldActor = ev->Sender;
                } else if (oldActor && ev->Sender != oldActor) {
                    newActor = ev->Sender;
                }
            }
            if (ev->InterconnectSession && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType
                && nodeId == 2) {
                if (captureOld && oldActor && ev->GetRecipientRewrite() == oldActor && !oldReply) {
                    oldSession = ev->InterconnectSession;
                    oldReply = std::move(ev);
                    return false;
                }
                if (newActor && ev->GetRecipientRewrite() == newActor) {
                    newSession = ev->InterconnectSession;
                }
            }
            if (captureOld && oldActor && ev->GetRecipientRewrite() == oldActor
                && ev->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
                oldTimers.push_back(std::move(ev));
                return false;
            }
            if (observePoison && ev->GetTypeRewrite() == TEvents::TSystem::Poison
                && ev->GetRecipientRewrite() == (newSession ? newSession : oldSession)) {
                ++sessionPoisons;
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT(parentAdvertisedDirectRootQueries);
        const auto rootBefore = QueryKeeper(runtime, fixture.Keepers.front(), true).GetScepter();
        captureOld = true;
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return oldReply && !oldTimers.empty(); }, TDuration::Seconds(60)),
            "did not observe a real heartbeat reply and its deadline");
        captureOld = false;
        if (rebind) {
            fixture.Disconnect(2, 1);
            UNIT_ASSERT(SimUntil(runtime, [&] { return !runtime.HasImmediateEvents(); }, TDuration::Seconds(1)));
            runtime.ReconnectNodes(2, 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return newActor && newSession; }, TDuration::Seconds(90)),
                "the child did not establish a new binding heartbeat after a session disconnect");
            UNIT_ASSERT(newActor != oldActor);
            UNIT_ASSERT(newSession != oldSession);
            UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == 1);
            runtime.Send(oldReply.release(), 2);
            ReplayDelayedEvents(runtime, oldTimers);
        } else {
            const ui64 cookie = oldReply->Cookie;
            runtime.Send(oldReply.release(), 2);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return !runtime.HasImmediateEvents(); }, TDuration::Seconds(1)),
                "the successful heartbeat reply was not processed");
            UNIT_ASSERT(std::ranges::all_of(oldTimers, [&](const auto& ev) { return ev->Cookie == cookie; }));
            ReplayDelayedEvents(runtime, oldTimers);
        }
        observePoison = true;
        const ui32 requestsBefore = heartbeatRequests;
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(sessionPoisons, 0);
        UNIT_ASSERT_VALUES_EQUAL(heartbeatRequests, requestsBefore);
        UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == 1);
        const auto rootAfter = QueryKeeper(runtime, fixture.Keepers.front(), true);
        UNIT_ASSERT(rootAfter.HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(rootAfter.GetScepter().GetId(), rootBefore.GetId());
        replaceEdge = runtime.AllocateEdgeActor(1);
        auto request = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, request.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(95)));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(BindingIgnoresRetiredDeadline) {
        CheckBindingLivenessIgnoresRetiredEvents(false);
    }

    Y_UNIT_TEST(BindingIgnoresEventsAfterRebind) {
        CheckBindingLivenessIgnoresRetiredEvents(true);
    }

    Y_UNIT_TEST(BindingResetsFailureStreak) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        TActorId heartbeatActor;
        TActorId session;
        THashSet<ui64> requests;
        THashSet<ui64> delayedCookies;
        THashSet<ui64> deadlines;
        THashSet<ui64> successfulReplies;
        ui32 sessionPoisons = 0;
        ui32 disconnections = 0;
        bool holdReplies = true;
        bool trackDisconnects = false;
        TActorId replaceEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedReplies;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, replaceEdge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasQueryWorkingRoot()
                && ev->Sender.NodeId() == 2 && ev->Sender != fixture.Keepers[1]) {
                if (!heartbeatActor) {
                    heartbeatActor = ev->Sender;
                }
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender, heartbeatActor);
                requests.insert(ev->Cookie);
            }
            if (heartbeatActor && ev->InterconnectSession
                && type == NStorage::TEvNodeConfigInvokeOnRootResult::EventType
                && ev->GetRecipientRewrite() == heartbeatActor) {
                const auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                UNIT_ASSERT_C(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                    result.DebugString());
                if (!session) {
                    session = ev->InterconnectSession;
                }
                UNIT_ASSERT_VALUES_EQUAL(ev->InterconnectSession, session);
                if (holdReplies && (requests.size() <= 2 || requests.size() == 4)) {
                    delayedCookies.insert(ev->Cookie);
                    delayedReplies.push_back(std::move(ev));
                    return false;
                }
                if (!delayedCookies.contains(ev->Cookie)) {
                    successfulReplies.insert(ev->Cookie);
                }
            }
            if (heartbeatActor && ev->GetRecipientRewrite() == heartbeatActor
                && type == TEvents::TSystem::Wakeup && delayedCookies.contains(ev->Cookie)) {
                deadlines.insert(ev->Cookie);
            }
            if (session && type == TEvents::TSystem::Poison && ev->GetRecipientRewrite() == session) {
                ++sessionPoisons;
            }
            if (trackDisconnects && type == TEvInterconnect::TEvNodeDisconnected::EventType
                && ((nodeId == 1 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 2)
                    || (nodeId == 2 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 1))) {
                ++disconnections;
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        const auto scepter = QueryKeeper(runtime, fixture.Keepers.front(), true).GetScepter();
        trackDisconnects = true;
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return successfulReplies.size() == 3; }, TDuration::Seconds(160)),
            "a successful heartbeat did not reset the earlier consecutive failures");
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(delayedReplies.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(deadlines.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sessionPoisons, 0);
        UNIT_ASSERT_VALUES_EQUAL(disconnections, 0);
        holdReplies = false;
        ReplayDelayedEvents(runtime, delayedReplies);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sessionPoisons, 0);
        UNIT_ASSERT_VALUES_EQUAL(disconnections, 0);
        UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == 1);
        const auto current = QueryKeeper(runtime, fixture.Keepers.front(), true);
        UNIT_ASSERT(current.HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(current.GetScepter().GetId(), scepter.GetId());
        replaceEdge = runtime.AllocateEdgeActor(1);
        auto request = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, request.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(95)));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(ReplaceThroughDeepTree) {
        constexpr ui32 nodeCount = 256;
        TCommitConfirmationFixture fixture(true, nodeCount);
        fixture.UseMirror3dcGroup(nodeCount);
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds[1] = 0;
        for (ui32 nodeId = 2; nodeId <= nodeCount; ++nodeId) {
            fixture.State.RequiredParentNodeIds[nodeId] = nodeId - 1;
        }
        auto& runtime = fixture.Runtime;
        bool holdGather = false;
        ui32 deepestScatterNode = 0;
        ui64 proposalGeneration = 0;
        std::optional<ui64> leafProposalCookie;
        bool heldProposalGather = false;
        std::vector<std::unique_ptr<IEventHandle>> delayedGathers;
        TActorId replaceEdge;
        TActorId queryEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> replaceResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> queryResponse;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, replaceEdge)) {
                return false;
            }
            if ((replaceEdge && CaptureInvokeResult(*ev, replaceEdge, replaceResponse))
                || (queryEdge && CaptureInvokeResult(*ev, queryEdge, queryResponse))) {
                return false;
            }
            if (ev->InterconnectSession && ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId() + 1, nodeId);
                deepestScatterNode = Max(deepestScatterNode, nodeId);
                const auto& scatter = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (nodeId == nodeCount && scatter.HasProposeStorageConfig()
                    && scatter.GetProposeStorageConfig().GetConfig().GetGeneration() == proposalGeneration) {
                    leafProposalCookie = scatter.GetCookie();
                }
            }
            if (holdGather && ev->Sender.NodeId() == nodeCount && ev->InterconnectSession
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType) {
                heldProposalGather |= leafProposalCookie
                    && ev->Get<NStorage::TEvNodeConfigGather>()->Record.GetCookie() == *leafProposalCookie;
                delayedGathers.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        Y_DEFER { runtime.FilterFunction = {}; };
        fixture.StartWithBoundChild(1, 2);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        std::vector<NStorage::TSuccessfulDisk> disks;
        const auto nodeWardenConfig = MakeNodeWardenConfig(fixture.Config);
        for (ui32 nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            disks.emplace_back(NStorage::TNodeIdentifier(fixture.Config.GetAllNodes(nodeId - 1)),
                "/dev/disk" + std::to_string(nodeId) + "_1", nodeId * 1000 + 1);
            if (nodeId == 78) {
                UNIT_ASSERT(!NStorage::HasConfigQuorum(fixture.Config, disks, TBridgePileId(),
                    *nodeWardenConfig, true));
            }
        }
        disks.pop_back();
        UNIT_ASSERT(NStorage::HasConfigQuorum(fixture.Config, disks, TBridgePileId(),
            *nodeWardenConfig, true));
        const auto replace = [&](ui64 version, bool silentLeaf) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return !runtime.HasImmediateEvents(); }, TDuration::Seconds(1)));
            replaceResponse.reset();
            queryResponse.reset();
            deepestScatterNode = 0;
            proposalGeneration = version + 2;
            leafProposalCookie.reset();
            heldProposalGather = false;
            UNIT_ASSERT(delayedGathers.empty());
            holdGather = silentLeaf;
            replaceEdge = runtime.AllocateEdgeActor(1);
            auto replace = fixture.MakeReplaceRequest(version, "mirror-3-dc");
            runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, replace.release()), 1);
            queryEdge = runtime.AllocateEdgeActor(1);
            auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            query->Record.MutableQueryConfig();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), queryEdge, query.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] {
                return replaceResponse && queryResponse && (!silentLeaf || heldProposalGather);
            }, TDuration::Seconds(90)),
                "deep tree blocked replace or its queued query");
            using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
            UNIT_ASSERT_C(replaceResponse->GetStatus() == TResult::OK, replaceResponse->DebugString());
            UNIT_ASSERT_C(queryResponse->GetStatus() == TResult::OK, queryResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(queryResponse->GetQueryConfig().GetConfig().GetGeneration(), version + 2);
            UNIT_ASSERT_VALUES_EQUAL(deepestScatterNode, nodeCount);
            UNIT_ASSERT_VALUES_EQUAL(!delayedGathers.empty(), silentLeaf);
            UNIT_ASSERT_VALUES_EQUAL(heldProposalGather, silentLeaf);
            const auto winner = queryResponse->GetQueryConfig().GetConfig();
            holdGather = false;
            ReplayDelayedEvents(runtime, delayedGathers);
            CheckDurableConfigConverges(fixture, winner);
        };
        replace(0, false);
        replace(1, true);
    }

    enum class EBridgeCommittedCatchup {
        DirectLeaf,
        BoundIntermediate,
        ColdIntermediate,
    };

    NKikimrBlobStorage::TStorageConfig MakeBridgeStorageConfig() {
        auto config = MakeStorageConfig(8);
        config.MutableSelfManagementConfig()->SetEnabled(true);
        config.MutableSelfManagementConfig()->SetErasureSpecies("mirror-3-dc");
        for (auto& node : *config.MutableAllNodes()) {
            node.MutableLocation()->SetBridgePileName(node.GetNodeId() <= 4 ? "pile0" : "pile1");
            node.MutableLocation()->SetDataCenter("dc-" + std::to_string(Min((node.GetNodeId() - 1) % 4 + 1, 3u)));
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
        AddMultiVDiskGroup(config, {1, 2, 3}, TBlobStorageGroupType::ErasureMirror3dc, 1);
        AddMultiVDiskGroup(config, {5, 6, 7}, TBlobStorageGroupType::ErasureMirror3dc, 2);
        for (ui32 pileIndex : {0u, 1u}) {
            auto *group = serviceSet->MutableGroups(pileIndex + 1);
            group->SetBridgeProxyGroupId(0);
            TBridgePileId::FromPileIndex(pileIndex).CopyToProto(group,
                &NKikimrBlobStorage::TGroupInfo::SetBridgePileId);
        }
        auto *cluster = config.MutableClusterState();
        cluster->CopyFrom(NBridge::MakeInitialClusterState(2));
        cluster->SetGeneration(1);
        auto *registry = config.MutableClusterStateDetails()->MutablePileRegistry();
        registry->AddPiles()->SetName("pile0");
        registry->AddPiles()->SetName("pile1");
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&config);
        return config;
    }

    Y_UNIT_TEST(BridgeRecoveryAdoptsPossiblyChosenCommittedConfig) {
        TCommitConfirmationFixture fixture(true, 8);
        fixture.Config = MakeBridgeStorageConfig();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        for (ui32 nodeId = 1; nodeId <= 8; ++nodeId) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        runtime.FilterFunction = [](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            return true;
        };
        fixture.Start(false, &bridge);
        auto updated = MakeNextConfig(fixture.Config);
        updated.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&updated);
        auto collected = MakeCollectedConfigs(fixture.Config, {2, 3, 4, 6, 7, 8});
        AddCollectedConfig(collected, fixture.Config, {3, 4, 7, 8});
        AddCollectedConfig(collected, updated, {2, 6});

        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {2, 3, 4, 6, 7, 8}, &bridge));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {3, 4, 7, 8}, &bridge));
        UNIT_ASSERT(!HasTestConfigQuorum(updated, {2, 6}, &bridge));
        UNIT_ASSERT(HasTestConfigQuorum(updated, {1, 2, 5, 6}, &bridge));
        const auto result = ProcessCollectedConfigs(fixture, collected);
        UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Root()).GetQueryConfig().GetConfig().GetFingerprint(),
            updated.GetFingerprint());
        CheckDurableConfigConverges(fixture, updated);
    }

    void CheckBridgeCommittedCatchup(EBridgeCommittedCatchup scenario) {
        auto config = MakeBridgeStorageConfig();
        const auto updated = MakeNextConfig(config);
        TNodeWardenTestState state;
        state.RequiredRootNodeId = 1;
        if (scenario != EBridgeCommittedCatchup::DirectLeaf) {
            state.RequiredParentNodeIds = {{7, 1}, {8, 7}};
        }
        const auto metadata = [&](ui32 nodeId) -> NKikimrBlobStorage::TPDiskMetadataRecord& {
            return state.Metadata[std::make_pair(nodeId, TString("/dev/disk" + std::to_string(nodeId) + "_1"))];
        };
        for (ui32 nodeId = 1; nodeId <= 8; ++nodeId) {
            metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(nodeId == 8 ? config : updated);
        }
        TTestActorSystem runtime(8);
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        UNIT_ASSERT(HasTestConfigQuorum(updated, {1, 2, 3, 4, 5, 6}, &bridge));
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        std::array<TActorId, 9> keepers;
        std::vector<TActorId> activeKeepers;
        bool blockCommitted = true;
        bool holdInitial = scenario == EBridgeCommittedCatchup::ColdIntermediate;
        bool sawLeafRequest = false;
        bool sawForwardedRequest = false;
        bool sawInitialRequestWithChild = false;
        bool sawMiddleWithoutCommitted = false;
        bool sawVolatileLeaf = false;
        bool leafLoaded = false;
        bool leafAccepted = false;
        std::vector<std::unique_ptr<IEventHandle>> heldInitial;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvNodeWardenStorageConfig::EventType) {
                const auto& msg = *ev->Get<TEvNodeWardenStorageConfig>();
                if (nodeId == 7 && msg.Config->GetFingerprint() == updated.GetFingerprint() && !msg.CommittedConfig) {
                    sawMiddleWithoutCommitted = true;
                }
                if (nodeId == 8 && msg.Config->GetFingerprint() == updated.GetFingerprint()
                    && (!msg.CommittedConfig || msg.CommittedConfig->GetGeneration() < 2)) {
                    sawVolatileLeaf = true;
                }
                leafLoaded |= nodeId == 8;
            }
            if (type == NStorage::TEvNodeWardenUpdateConfigFromPeer::EventType && nodeId == 8
                && scenario != EBridgeCommittedCatchup::ColdIntermediate) {
                blockCommitted = false;
            }
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Get<NStorage::TEvNodeConfigReversePush>()->Record
                    .GetCommittedStorageConfig().GetGeneration() == 2
                && nodeId == (scenario == EBridgeCommittedCatchup::DirectLeaf ? 8u : 7u)
                && blockCommitted) {
                return false;
            }
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigReversePush::EventType
                && nodeId == 8 && ev->Sender.NodeId() == 7
                && !ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetRejected()) {
                leafAccepted = true;
            }
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigPush::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigPush>()->Record;
                if (ev->Sender.NodeId() == 7 && nodeId == 1 && record.GetInitial() && holdInitial) {
                    heldInitial.push_back(std::move(ev));
                    return false;
                }
                if (record.GetRequestedCommittedConfigGeneration() == 2) {
                    sawLeafRequest |= ev->Sender.NodeId() == 8;
                    sawForwardedRequest |= ev->Sender.NodeId() == 7 && nodeId == 1 && !record.GetInitial();
                    sawInitialRequestWithChild |= ev->Sender.NodeId() == 7 && nodeId == 1 && record.GetInitial()
                        && std::ranges::any_of(record.GetBoundNodes(), [](const auto& item) {
                            return item.GetNodeId().GetNodeId() == 8;
                        });
                }
            }
            return true;
        };
        const auto addKeeper = [&](ui32 nodeId) {
            keepers[nodeId] = RegisterKeeper(runtime, config, nodeId, state, {}, &bridge);
            activeKeepers.push_back(keepers[nodeId]);
        };
        for (ui32 nodeId = 1; nodeId <= 6; ++nodeId) {
            addKeeper(nodeId);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, activeKeepers) == 1; }));
        UNIT_ASSERT(QueryKeeper(runtime, keepers[1], true).HasScepter());
        addKeeper(7);
        if (scenario == EBridgeCommittedCatchup::ColdIntermediate) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return !heldInitial.empty() && sawMiddleWithoutCommitted; }));
        } else {
            UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, activeKeepers) == 1; }));
        }
        addKeeper(8);
        if (scenario == EBridgeCommittedCatchup::ColdIntermediate) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return leafLoaded; }, TDuration::Seconds(1)));
            UNIT_ASSERT(runtime.WrapInActorContext(keepers[8], [&](IActor *actor) {
                auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
                keeper->AbortBinding("attach child before parent's outgoing binding", true, false);
                keeper->StartBinding(7);
            }));
            UNIT_ASSERT(SimUntil(runtime, [&] { return leafAccepted; }, TDuration::Seconds(1)));
        } else {
            UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, activeKeepers) == 1; }));
        }
        runtime.Sim([&] { return runtime.HasImmediateEvents(); });
        UNIT_ASSERT_VALUES_EQUAL(metadata(8).GetCommittedStorageConfig().GetGeneration(), 1);
        if (scenario != EBridgeCommittedCatchup::DirectLeaf) {
            UNIT_ASSERT(sawMiddleWithoutCommitted);
            UNIT_ASSERT_VALUES_EQUAL(metadata(7).GetCommittedStorageConfig().GetFingerprint(), updated.GetFingerprint());
        }
        runtime.Send(new IEventHandle(keepers[8], {},
            new NStorage::TEvNodeWardenUpdateConfigFromPeer(updated, updated)), 8);
        if (scenario == EBridgeCommittedCatchup::ColdIntermediate) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return sawLeafRequest && sawForwardedRequest && sawVolatileLeaf; },
                TDuration::Seconds(1)));
            UNIT_ASSERT_VALUES_EQUAL(metadata(8).GetCommittedStorageConfig().GetGeneration(), 1);
            holdInitial = false;
            blockCommitted = false;
            heldInitial.clear();
            UNIT_ASSERT(runtime.WrapInActorContext(keepers[7], [&](IActor *actor) {
                auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
                keeper->AbortBinding("retry after child requested committed configuration", false, false);
                keeper->StartBinding(1);
            }));
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return metadata(8).GetCommittedStorageConfig().GetFingerprint() == updated.GetFingerprint();
        }, TDuration::Seconds(35)), "bridge peer's volatile configuration was not persisted as committed");
        UNIT_ASSERT(sawVolatileLeaf);
        UNIT_ASSERT(sawLeafRequest);
        if (scenario == EBridgeCommittedCatchup::BoundIntermediate) {
            UNIT_ASSERT(sawForwardedRequest);
        } else if (scenario == EBridgeCommittedCatchup::ColdIntermediate) {
            UNIT_ASSERT(sawInitialRequestWithChild);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, activeKeepers) == 1; }));
        UNIT_ASSERT_C(!SimUntil(runtime, [&] {
            return std::ranges::any_of(activeKeepers, [&](TActorId id) {
                return metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() != updated.GetFingerprint();
            });
        }, TDuration::Seconds(3)), "bridge committed configuration regressed after catchup");
    }

    enum class EBridgeRecoveryCollect {
        Complete,
        PartialWithProposal,
        PartialWithoutProposal,
    };

    void CheckBridgeFencesOldQuorum(EBridgeRecoveryCollect collect) {
        TCommitConfirmationFixture fixture(true, 8);
        fixture.Config = MakeBridgeStorageConfig();
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {5, 1}, {6, 5}};
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        for (ui32 nodeId = 1; nodeId <= 8; ++nodeId) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        fixture.Keepers.resize(8);
        bool holdGather = false;
        bool pauseRoot = false;
        bool holdReads = false;
        bool sawCollectedOldProposal = false;
        ui32 prematureProposals = 0;
        ui64 oldRootAppliedGeneration = 0;
        TString oldRootAppliedFingerprint;
        TActorId oldEdge;
        TActorId newEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> oldResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> newResponse;
        THashSet<ui32> heldVoters;
        THashMap<ui64, TString> appliedByGeneration;
        std::vector<std::unique_ptr<IEventHandle>> heldGathers;
        std::vector<std::unique_ptr<IEventHandle>> heldRootEvents;
        std::vector<std::unique_ptr<IEventHandle>> heldReads;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if ((oldEdge && CaptureInvokeResult(*ev, oldEdge, oldResponse))
                || (newEdge && CaptureInvokeResult(*ev, newEdge, newResponse))) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvNodeWardenStorageConfig::EventType) {
                const auto& config = *ev->Get<TEvNodeWardenStorageConfig>()->Config;
                const auto [it, inserted] = appliedByGeneration.emplace(config.GetGeneration(), config.GetFingerprint());
                UNIT_ASSERT_C(inserted || it->second == config.GetFingerprint(),
                    "different bridge configurations reached NodeWarden at generation " << config.GetGeneration());
                if (nodeId == 1) {
                    oldRootAppliedGeneration = config.GetGeneration();
                    oldRootAppliedFingerprint = config.GetFingerprint();
                }
            }
            if (holdReads && type == NStorage::TEvNodeWardenReadMetadataResult::EventType
                && (nodeId == 5 || nodeId == 6
                    || (nodeId == 2 && collect == EBridgeRecoveryCollect::PartialWithoutProposal))) {
                heldReads.push_back(std::move(ev));
                return false;
            }
            if (pauseRoot && nodeId == 8 && type == NStorage::TEvNodeConfigGather::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigGather>()->Record;
                sawCollectedOldProposal |= record.HasCollectConfigs()
                    && std::ranges::any_of(record.GetCollectConfigs().GetProposedConfigs(), [](const auto& item) {
                        return item.GetConfig().GetGeneration() == 2;
                    });
            }
            if (holdReads && type == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasProposeStorageConfig()) {
                ++prematureProposals;
            }
            if (holdGather && ev->InterconnectSession && ev->GetRecipientRewrite() == fixture.Keepers.front()
                && type == NStorage::TEvNodeConfigGather::EventType
                && ev->Get<NStorage::TEvNodeConfigGather>()->Record.HasProposeStorageConfig()) {
                for (const auto& status : ev->Get<NStorage::TEvNodeConfigGather>()->Record
                    .GetProposeStorageConfig().GetStatus()) {
                    if (status.SuccessfulDrivesSize()) {
                        heldVoters.insert(status.GetNodeId().GetNodeId());
                    }
                }
                heldGathers.push_back(std::move(ev));
                return false;
            }
            if (pauseRoot && ev->GetRecipientRewrite() == fixture.Keepers.front()) {
                heldRootEvents.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {1, 2, 5, 6}, &bridge));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {2, 5, 6}, &bridge));
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, {2, 3, 4, 5, 6, 7, 8}, &bridge));
        runtime.Start();
        const auto add = [&](ui32 id) {
            fixture.Keepers[id - 1] = RegisterKeeper(runtime, fixture.Config, id, fixture.State, {}, &bridge);
            return fixture.Keepers[id - 1];
        };
        std::vector<TActorId> oldKeepers;
        for (ui32 id : {1, 2, 5, 6}) {
            oldKeepers.push_back(add(id));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, oldKeepers) == 1; }));
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig()
            .GetGeneration(), 1);
        holdGather = true;
        oldEdge = runtime.AllocateEdgeActor(1);
        auto oldRequest = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        oldRequest->Record.MutableUpdateConfig()->MutableConfig()->CopyFrom(fixture.Config);
        oldRequest->Record.MutableUpdateConfig()->MutableConfig()->MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), oldEdge, oldRequest.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return heldVoters.contains(2) && heldVoters.contains(5) && heldVoters.contains(6)
                && std::ranges::all_of(oldKeepers, [&](TActorId id) {
                    return fixture.Metadata(id.NodeId()).GetProposedStorageConfig().GetGeneration() == 2;
                });
        }, TDuration::Seconds(10)), "the old root never obtained the real physical proposal quorum");
        runtime.Sim([&] { return runtime.HasImmediateEvents(); });
        const auto oldProposal = fixture.Metadata(1).GetProposedStorageConfig();
        UNIT_ASSERT(!oldResponse);
        UNIT_ASSERT_VALUES_EQUAL(oldRootAppliedGeneration, 1);
        pauseRoot = true;
        holdReads = collect != EBridgeRecoveryCollect::Complete;
        fixture.State.RequiredParentNodeIds = {{8, 0}};
        fixture.State.RequiredRootNodeId = 8;
        for (ui32 id : {3, 4, 7, 8}) {
            add(id);
        }
        std::vector<TActorId> healthy(fixture.Keepers.begin() + 1, fixture.Keepers.end());
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, healthy) == 8; },
            TDuration::Seconds(180)), "the healthy bridge quorum did not replace the paused root");
        auto recovered = QueryKeeper(runtime, fixture.Keepers[7]).GetQueryConfig().GetConfig();
        if (holdReads) {
            UNIT_ASSERT_VALUES_EQUAL(recovered.GetGeneration(), 1);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(recovered.SerializeAsString(), oldProposal.SerializeAsString());
            UNIT_ASSERT(recovered.GetSelfManagementConfig().GetAutomaticBootstrap());
        }
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        const auto update = [&](bool advanceOnly) {
            newResponse.reset();
            newEdge = runtime.AllocateEdgeActor(8);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            if (advanceOnly) {
                request->Record.MutableAdvanceGeneration();
            } else {
                auto *config = request->Record.MutableUpdateConfig()->MutableConfig();
                config->CopyFrom(recovered);
                config->MutableSelfManagementConfig()->SetAutomaticBootstrap(false);
            }
            runtime.Send(new IEventHandle(fixture.Keepers[7], newEdge, request.release()), 8);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return newResponse.has_value(); }, TDuration::Seconds(65)),
                "bridge mutation waited indefinitely for a fresh metadata quorum");
        };
        if (holdReads) {
            UNIT_ASSERT(!heldReads.empty());
            UNIT_ASSERT_VALUES_EQUAL(sawCollectedOldProposal, collect == EBridgeRecoveryCollect::PartialWithProposal);
            UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config,
                collect == EBridgeRecoveryCollect::PartialWithProposal
                    ? std::vector<ui32>{2, 3, 4, 7, 8} : std::vector<ui32>{3, 4, 7, 8}, &bridge));
            for (bool advanceOnly : {true, false}) {
                update(advanceOnly);
                UNIT_ASSERT_C(newResponse->GetStatus() == TResult::NO_QUORUM || newResponse->GetStatus() == TResult::RACE,
                    newResponse->DebugString());
                UNIT_ASSERT_VALUES_EQUAL(prematureProposals, 0);
            }
            holdReads = false;
            ReplayDelayedEvents(runtime, heldReads);
        }
        for (ui32 attempt = 0; attempt != 3; ++attempt) {
            update(true);
            if (newResponse->GetStatus() == TResult::OK) {
                break;
            }
            UNIT_ASSERT_C(newResponse->GetStatus() == TResult::RACE, newResponse->DebugString());
        }
        UNIT_ASSERT_C(newResponse->GetStatus() == TResult::OK, newResponse->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers[7]).GetQueryConfig().GetConfig();
        UNIT_ASSERT(winner.GetGeneration() > oldProposal.GetGeneration());
        UNIT_ASSERT(winner.GetFingerprint() != oldProposal.GetFingerprint());
        UNIT_ASSERT_C(winner.GetSelfManagementConfig().GetAutomaticBootstrap(),
            "the new bridge quorum discarded the body accepted by the old physical proposal quorum");
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return std::ranges::all_of(healthy, [&](TActorId id) {
                return fixture.Metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint();
            });
        }));
        pauseRoot = holdGather = false;
        ReplayDelayedEvents(runtime, heldGathers);
        ReplayDelayedEvents(runtime, heldRootEvents);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return oldResponse.has_value() && !runtime.HasImmediateEvents();
        }, TDuration::Seconds(1)));
        UNIT_ASSERT_C(oldRootAppliedGeneration != winner.GetGeneration()
            || oldRootAppliedFingerprint == winner.GetFingerprint(),
            "two roots committed different bridge configurations at the same generation: old="
            << NStorage::TStorageConfigMeta(oldProposal).ShortDebugString()
            << " new=" << NStorage::TStorageConfigMeta(winner).ShortDebugString());
        runtime.ReconnectNodes(1, 2);
        runtime.ReconnectNodes(1, 5);
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return std::ranges::all_of(fixture.Keepers, [&](TActorId id) {
                return fixture.State.NodeWardenConfigs.at(id.NodeId())->GetFingerprint() == winner.GetFingerprint();
            });
        }));
    }

    Y_UNIT_TEST(BridgeFencesOldQuorum) {
        CheckBridgeFencesOldQuorum(EBridgeRecoveryCollect::Complete);
    }

    Y_UNIT_TEST(BridgeFencesOldQuorumAfterTimeout) {
        CheckBridgeFencesOldQuorum(EBridgeRecoveryCollect::PartialWithProposal);
        CheckBridgeFencesOldQuorum(EBridgeRecoveryCollect::PartialWithoutProposal);
    }

    Y_UNIT_TEST(BridgeFailoverRecoversCommitted) {
        TCommitConfirmationFixture fixture(true, 8);
        fixture.Config = MakeBridgeStorageConfig();
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds[1] = 0;
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        auto committed = MakeNextConfig(fixture.Config);
        committed.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&committed);
        for (ui32 nodeId : {1, 2, 3, 4}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(nodeId == 1 ? fixture.Config : committed);
        }
        THashSet<ui32> boundNodes;
        ui32 proposals = 0;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            if (ev->InterconnectSession && ev->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType
                && ev->Sender.NodeId() == 1 && !ev->Get<NStorage::TEvNodeConfigReversePush>()->Record.GetRejected()) {
                boundNodes.insert(nodeId);
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasProposeStorageConfig()) {
                ++proposals;
            }
            return true;
        };
        runtime.Start();
        for (ui32 nodeId : {1, 2, 3, 4}) {
            fixture.Keepers.push_back(RegisterKeeper(runtime, fixture.Config, nodeId, fixture.State, {}, &bridge));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return boundNodes.size() == 3 && fixture.State.NodeWardenConfigs.size() == 4;
        }));
        UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig()
            .GetFingerprint(), fixture.Config.GetFingerprint());
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {1, 2, 3, 4}, &bridge));
        auto failover = fixture.Config;
        failover.MutableClusterState()->SetGeneration(failover.GetClusterState().GetGeneration() + 1);
        failover.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        UNIT_ASSERT(HasTestConfigQuorum(failover, {1, 2, 3, 4}, &bridge));
        const auto switchState = [&] {
            response.reset();
            edge = runtime.AllocateEdgeActor(1);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(failover.GetClusterState());
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)),
                "bridge failover waited for the unavailable pile");
        };
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        switchState();
        UNIT_ASSERT_C(response->GetStatus() == TResult::RACE, response->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(proposals, 0);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return fixture.State.NodeWardenConfigs.at(1)->GetFingerprint() == committed.GetFingerprint();
        }));
        switchState();
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT(winner.GetGeneration() > committed.GetGeneration());
        UNIT_ASSERT(winner.GetSelfManagementConfig().GetAutomaticBootstrap());
        auto expectedClusterState = failover.GetClusterState();
        NStorage::UpdateClusterStateGuid(&expectedClusterState);
        UNIT_ASSERT_VALUES_EQUAL(winner.GetClusterState().SerializeAsString(), expectedClusterState.SerializeAsString());
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return std::ranges::all_of(fixture.Keepers, [&](TActorId id) {
                return fixture.State.NodeWardenConfigs.at(id.NodeId())->GetFingerprint() == winner.GetFingerprint();
            });
        }));
    }

    Y_UNIT_TEST(BridgeFailoverRecoversPossiblyChosenProposalWithoutScepter) {
        TCommitConfirmationFixture fixture(true, 8);
        fixture.Config = MakeBridgeStorageConfig();
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 1}};
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        for (ui32 nodeId : {1, 2, 3, 4}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        fixture.Keepers.resize(8);
        auto failover = fixture.Config;
        failover.MutableClusterState()->SetGeneration(2);
        failover.MutableClusterState()->SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        const auto hasNodeQuorum = [&](const auto& config, std::initializer_list<ui32> nodeIds) {
            std::vector<NStorage::TNodeIdentifier> nodes;
            for (ui32 nodeId : nodeIds) {
                nodes.emplace_back(config.GetAllNodes(nodeId - 1));
            }
            return NStorage::HasNodeQuorum(config, nodes, TBridgePileId(),
                *MakeNodeWardenConfig(config, &bridge), nullptr, false);
        };
        UNIT_ASSERT(!hasNodeQuorum(fixture.Config, {1, 2, 3}));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {1, 2, 3}, &bridge));
        UNIT_ASSERT(hasNodeQuorum(failover, {1, 2, 3}));
        UNIT_ASSERT(HasTestConfigQuorum(failover, {1, 2, 3}, &bridge));
        UNIT_ASSERT(hasNodeQuorum(failover, {2, 3, 4}));
        UNIT_ASSERT(HasTestConfigQuorum(failover, {2, 3, 4}, &bridge));
        UNIT_ASSERT(!hasNodeQuorum(failover, {2, 3}));
        UNIT_ASSERT(!HasTestConfigQuorum(failover, {2, 3}, &bridge));

        bool recovering = false;
        bool holdRecoveryWrites = true;
        bool publishedBeforeRecoveryQuorum = false;
        THashSet<ui32> boundNodes;
        THashSet<ui32> delayedPublicationNodes;
        THashSet<ui32> delayedRecoveryVoters;
        TActorId oldEdge;
        TActorId newEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> oldResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> newResponse;
        std::vector<std::unique_ptr<IEventHandle>> delayedPublications;
        std::vector<std::unique_ptr<IEventHandle>> delayedRecoveryWrites;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if ((oldEdge && CaptureInvokeResult(*ev, oldEdge, oldResponse))
                || (newEdge && CaptureInvokeResult(*ev, newEdge, newResponse))) {
                return false;
            }
            const auto type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeConfigReversePush::EventType && ev->InterconnectSession) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigReversePush>()->Record;
                if (ev->Sender.NodeId() == 1 && !record.GetRejected()) {
                    boundNodes.insert(nodeId);
                }
                if ((nodeId == 2 || nodeId == 3) && record.GetCommittedStorageConfig().GetGeneration() == 2) {
                    delayedPublicationNodes.insert(nodeId);
                    delayedPublications.push_back(std::move(ev));
                    return false;
                }
            }
            if (recovering && holdRecoveryWrites && type == TEvNodeWardenStorageConfig::EventType) {
                publishedBeforeRecoveryQuorum |= ev->Get<TEvNodeWardenStorageConfig>()->Config->GetGeneration() > 1;
            }
            if (recovering && holdRecoveryWrites && (nodeId == 2 || nodeId == 3)
                && type == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetProposedStorageConfig().GetGeneration() == 2) {
                delayedRecoveryVoters.insert(nodeId);
                delayedRecoveryWrites.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        runtime.Start();
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Keepers[nodeId - 1] = RegisterKeeper(runtime, fixture.Config, nodeId, fixture.State, {}, &bridge);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return boundNodes.size() == 2 && fixture.State.NodeWardenConfigs.size() == 3; }));
        UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers[0], true).HasScepter());
        oldEdge = runtime.AllocateEdgeActor(1);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(failover.GetClusterState());
        runtime.Send(new IEventHandle(fixture.Keepers[0], oldEdge, request.release()), 1);
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return oldResponse && delayedPublicationNodes.size() == 2
                && fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration() == 2;
        }, TDuration::Seconds(35)), "the original bridge failover did not reach a physical proposal quorum");
        UNIT_ASSERT_C(oldResponse->GetStatus() == TResult::OK, oldResponse->DebugString());
        const auto accepted = fixture.Metadata(2).GetProposedStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(accepted.GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetProposedStorageConfig().GetFingerprint(), accepted.GetFingerprint());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);

        runtime.StopNode(1);
        recovering = true;
        fixture.State.RequiredRootNodeId = 4;
        fixture.State.RequiredParentNodeIds = {{2, 4}, {3, 4}, {4, 0}};
        fixture.Keepers[3] = RegisterKeeper(runtime, fixture.Config, 4, fixture.State, {}, &bridge);
        const std::vector<TActorId> survivors{fixture.Keepers[1], fixture.Keepers[2], fixture.Keepers[3]};
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(survivors, [&](TActorId id) {
                ui32 rootNodeId = 0;
                runtime.WrapInActorContext(id, [&](IActor *actor) {
                    rootNodeId = static_cast<NStorage::TDistributedConfigKeeper*>(actor)->GetRootNodeId();
                });
                return rootNodeId == 4;
            });
        }, TDuration::Seconds(90), 4), "the surviving pile did not bind to the stale root");
        UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers[3], true).HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers[3]).GetQueryConfig().GetConfig().GetGeneration(), 1);
        UNIT_ASSERT(!hasNodeQuorum(fixture.Config, {2, 3, 4}));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {2, 3, 4}, &bridge));
        newEdge = runtime.AllocateEdgeActor(4);
        request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(failover.GetClusterState());
        runtime.Send(new IEventHandle(fixture.Keepers[3], newEdge, request.release()), 4);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return publishedBeforeRecoveryQuorum || delayedRecoveryVoters.size() == 2 || newResponse;
        }, TDuration::Seconds(35), 4), "the bridge invoke did not recover its possibly chosen proposal without a scepter");
        UNIT_ASSERT_C(!publishedBeforeRecoveryQuorum, "bridge recovery published the accepted body before a fresh quorum");
        UNIT_ASSERT_C(!newResponse, (newResponse ? newResponse->DebugString() : TString()));
        UNIT_ASSERT_VALUES_EQUAL(delayedRecoveryVoters.size(), 2);
        for (ui32 nodeId : {2, 3, 4}) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.NodeWardenConfigs.at(nodeId)->GetGeneration(), 1);
        }
        holdRecoveryWrites = false;
        ReplayDelayedEvents(runtime, delayedRecoveryWrites);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return newResponse.has_value(); }, TDuration::Seconds(35), 4),
            "bridge recovery did not finish after the target quorum accepted its proposal");
        UNIT_ASSERT_C(newResponse->GetStatus() == TResult::RACE, newResponse->DebugString());
        const auto recovered = QueryKeeper(runtime, fixture.Keepers[3]).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(recovered.SerializeAsString(), accepted.SerializeAsString());
        auto acceptedBody = accepted;
        acceptedBody.ClearGeneration();
        acceptedBody.ClearFingerprint();
        acceptedBody.ClearPrevConfig();

        for (ui32 attempt = 0; attempt != 3; ++attempt) {
            newResponse.reset();
            request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableAdvanceGeneration();
            runtime.Send(new IEventHandle(fixture.Keepers[3], newEdge, request.release()), 4);
            UNIT_ASSERT(SimUntil(runtime, [&] { return newResponse.has_value(); }, TDuration::Seconds(35), 4));
            if (newResponse->GetStatus() == TResult::OK) {
                break;
            }
            UNIT_ASSERT_C(newResponse->GetStatus() == TResult::RACE, newResponse->DebugString());
        }
        UNIT_ASSERT_C(newResponse->GetStatus() == TResult::OK, newResponse->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers[3]).GetQueryConfig().GetConfig();
        UNIT_ASSERT(winner.GetGeneration() > recovered.GetGeneration());
        auto winnerBody = winner;
        winnerBody.ClearGeneration();
        winnerBody.ClearFingerprint();
        winnerBody.ClearPrevConfig();
        UNIT_ASSERT_VALUES_EQUAL(winnerBody.SerializeAsString(), acceptedBody.SerializeAsString());
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(survivors, [&](TActorId id) {
                return fixture.Metadata(id.NodeId()).GetCommittedStorageConfig().GetFingerprint() == winner.GetFingerprint()
                    && fixture.State.NodeWardenConfigs.at(id.NodeId())->GetFingerprint() == winner.GetFingerprint();
            });
        }, TDuration::Seconds(30), 4), "the surviving pile did not persist and publish the recovered bridge body");
    }

    Y_UNIT_TEST(BridgeAdvanceBypassesPausedIntermediate) {
        constexpr ui32 middleNodeId = 8;
        TCommitConfirmationFixture fixture(true, 8);
        fixture.Config = MakeBridgeStorageConfig();
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 1}, {4, 1},
                                              {5, 8}, {6, 8}, {7, 8}, {8, 1}};
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");
        for (ui32 nodeId = 1; nodeId <= 8; ++nodeId) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        bool pauseMiddle = false;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        std::vector<std::unique_ptr<IEventHandle>> delayedEvents;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            if (pauseMiddle && ev->GetRecipientRewrite() == fixture.Keepers[middleNodeId - 1]) {
                delayedEvents.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.Start(false, &bridge);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        std::vector<TActorId> healthyKeepers;
        std::vector<ui32> healthyNodeIds;
        for (const auto keeper : fixture.Keepers) {
            if (keeper.NodeId() != middleNodeId) {
                healthyKeepers.push_back(keeper);
                healthyNodeIds.push_back(keeper.NodeId());
            }
        }
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, healthyNodeIds, &bridge));
        UNIT_ASSERT(!HasTestConfigQuorum(fixture.Config, {1, 2, 3, 4}, &bridge));
        fixture.State.RequiredParentNodeIds.clear();
        pauseMiddle = true;
        const TInstant deadline = runtime.GetClock() + TDuration::Seconds(180);
        ui32 attempts = 0;
        const auto advance = [&] {
            ++attempts;
            response.reset();
            edge = runtime.AllocateEdgeActor(1);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableAdvanceGeneration();
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
            UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(95)),
                "a paused keeper blocked bridge configuration rollout");
        };
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        do {
            advance();
        } while (response->GetStatus() != TResult::OK && runtime.GetClock() < deadline);
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK,
            "a healthy bridge pile quorum remained behind the paused keeper after " << attempts
            << " attempts: " << response->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(healthyKeepers, [&](TActorId keeper) {
                return fixture.Metadata(keeper.NodeId()).GetCommittedStorageConfig().GetFingerprint()
                    == winner.GetFingerprint();
            });
        }), "bridge committed configuration did not reach the healthy metadata disks");
        UNIT_ASSERT(FindConvergedRoot(runtime, healthyKeepers) == 1);
        UNIT_ASSERT(!delayedEvents.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(middleNodeId).GetCommittedStorageConfig().GetGeneration(), 1);
        pauseMiddle = false;
        ReplayDelayedEvents(runtime, delayedEvents);
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT(FindConvergedRoot(runtime, fixture.Keepers) == 1);
        for (const auto keeper : fixture.Keepers) {
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, keeper, true).HasScepter(), keeper.NodeId() == 1);
        }
        advance();
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto advanced = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(advanced.GetGeneration(), 3);
        CheckDurableConfigConverges(fixture, advanced);
    }

    Y_UNIT_TEST(BridgePeerConfigCatchesUpDisk) {
        CheckBridgeCommittedCatchup(EBridgeCommittedCatchup::DirectLeaf);
    }

    Y_UNIT_TEST(BridgeCatchupThroughColdCache) {
        CheckBridgeCommittedCatchup(EBridgeCommittedCatchup::BoundIntermediate);
    }

    Y_UNIT_TEST(BridgeCatchupBeforeParentBinding) {
        CheckBridgeCommittedCatchup(EBridgeCommittedCatchup::ColdIntermediate);
    }

    Y_UNIT_TEST(ReassignVDiskStatusTimeoutReleasesQueue) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& runtime = fixture.Runtime;
        runtime.RegisterService(MakeBlobStorageVDiskID(1, 1, 0), runtime.Register(new TIgnoreActor, 1));
        const TActorId root = fixture.Root();
        const TActorId reassignEdge = runtime.AllocateEdgeActor(1);
        const TActorId queryEdge = runtime.AllocateEdgeActor(1);
        const TVDiskID vdiskId(TGroupId::Zero(), 1, 0, 0, 0);
        TActorId handler;
        ui32 statusRequests = 0;
        ui32 reassignReplies = 0;
        bool baseConfigReplied = false;
        std::unique_ptr<IEventHandle> lateStatus;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> reassignResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> queryResponse;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (CaptureInvokeResult(*ev, reassignEdge, reassignResponse)) {
                ++reassignReplies;
                return false;
            }
            if (CaptureInvokeResult(*ev, queryEdge, queryResponse)) {
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVStatus::EventType) {
                ++statusRequests;
                handler = ev->Sender;
                lateStatus = std::make_unique<IEventHandle>(handler, ev->Recipient,
                    new TEvBlobStorage::TEvVStatusResult(NKikimrProto::OK, vdiskId, true, true, false, 1));
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenQueryBaseConfig::EventType) {
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, new NStorage::TEvNodeWardenBaseConfig), nodeId);
                baseConfigReplied = true;
                return false;
            }
            return true;
        };
        auto reassign = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        VDiskIDFromVDiskID(vdiskId, reassign->Record.MutableReassignGroupDisk()->MutableVDiskId());
        runtime.Send(new IEventHandle(root, reassignEdge, reassign.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return statusRequests && baseConfigReplied; }),
            "status requests=" << statusRequests << " base config replied=" << baseConfigReplied
            << " response=" << (reassignResponse ? reassignResponse->DebugString() : "none"));
        UNIT_ASSERT_VALUES_EQUAL(statusRequests, 1);
        auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        query->Record.MutableQueryConfig();
        runtime.Send(new IEventHandle(root, queryEdge, query.release()), 1);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(25));
        UNIT_ASSERT(!reassignResponse && !queryResponse);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return reassignResponse && queryResponse; }, TDuration::Seconds(10)),
            "A lost VStatus reply blocked the configuration queue");
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(reassignResponse->GetStatus() == TResult::ERROR, reassignResponse->DebugString());
        UNIT_ASSERT(TString(reassignResponse->GetErrorReason()).Contains("Timed out waiting for VDisk status"));
        UNIT_ASSERT_C(queryResponse->GetStatus() == TResult::OK, queryResponse->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(queryResponse->GetQueryConfig().GetConfig().GetGeneration(), 1);
        UNIT_ASSERT(!runtime.GetActor(handler));
        runtime.Send(lateStatus.release(), 1);
        QueryKeeper(runtime, root);
        UNIT_ASSERT_VALUES_EQUAL(reassignReplies, 1);
        CheckDurableConfigConverges(fixture, fixture.Config);
    }

    void CheckConsolePublishesLatestConfig(bool delayPropose) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto *stateStorage = fixture.Config.MutableStateStorageConfig();
        stateStorage->SetSSId(1);
        stateStorage->MutableRing()->SetNToSelect(1);
        stateStorage->MutableRing()->AddNode(1);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        auto& runtime = fixture.Runtime;
        TActorId tabletServer;
        ui32 validations = 0;
        ui32 proposes = 0;
        ui32 commits = 0;
        TString publishedYaml;
        std::unique_ptr<IEventHandle> firstProposeResponse;
        std::unique_ptr<IEventHandle> firstCommitResponse;
        std::unique_ptr<IEventHandle> duplicateCommitResponse;
        std::unique_ptr<IEventHandle> secondProposeResponse;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (!tabletServer) {
                tabletServer = runtime.Register(new TIgnoreActor, 1);
                runtime.RegisterService(MakeTabletResolverID(), runtime.Register(new TIgnoreActor, 1));
            }
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvTabletResolver::TEvForward::EventType) {
                const ui64 tabletId = ev->Get<TEvTabletResolver::TEvForward>()->TabletID;
                if (tabletId == MakeBSControllerID() || tabletId == MakeConsoleID()) {
                    runtime.Send(new IEventHandle(ev->Sender, tabletServer,
                        new TEvTabletResolver::TEvForwardResult(tabletId, tabletServer, tabletServer, 0)), nodeId);
                    return false;
                }
            } else if (type == TEvTabletPipe::TEvConnect::EventType && ev->GetRecipientRewrite() == tabletServer) {
                const ui64 tabletId = ev->Get<TEvTabletPipe::TEvConnect>()->Record.GetTabletId();
                runtime.Send(new IEventHandle(ev->Sender, tabletServer, new TEvTabletPipe::TEvConnectResult(
                    NKikimrProto::OK, tabletId, ev->Sender, tabletServer, true, 1, TString()), 0, ev->Cookie), nodeId);
                return false;
            }
            if (type != TEvTabletPipe::EvSend) {
                return true;
            }
            std::unique_ptr<IEventBase> response;
            if (ev->Type == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerConfigResponse>();
                result->Record.MutableResponse()->AddStatus()->SetInterfaceVersion(BSC_INTERFACE_DISTCONF_CONTROL);
                response = std::move(result);
            } else if (ev->Type == TEvBlobStorage::TEvControllerDistconfRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerDistconfResponse>();
                result->Record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                response = std::move(result);
            } else if (ev->Type == TEvBlobStorage::TEvControllerValidateConfigRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
                result->Record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
                result->Record.SetYAML(fixture.MakeReplacementYaml(++validations, "mirror-3-dc"));
                response = std::move(result);
            } else if (ev->Type == TEvBlobStorage::TEvControllerProposeConfigRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigResponse>();
                if (ev->Get<TEvBlobStorage::TEvControllerProposeConfigRequest>()->Record.HasConfigHash()) {
                    ++proposes;
                    result->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded);
                    if (delayPropose && proposes == 1) {
                        firstProposeResponse = std::make_unique<IEventHandle>(ev->Sender, tabletServer,
                            result.release(), 0, ev->Cookie);
                        return false;
                    }
                    if (proposes == 2) {
                        secondProposeResponse = std::make_unique<IEventHandle>(ev->Sender, tabletServer,
                            result.release(), 0, ev->Cookie);
                        return false;
                    }
                } else {
                    result->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::ReverseCommit);
                }
                response = std::move(result);
            } else if (ev->Type == TEvBlobStorage::TEvControllerConsoleCommitRequest::EventType) {
                ++commits;
                publishedYaml = ev->Get<TEvBlobStorage::TEvControllerConsoleCommitRequest>()->Record.GetYAML();
                auto result = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
                result->Record.SetStatus(NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed);
                if (commits == 1) {
                    auto duplicate = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
                    duplicate->Record.CopyFrom(result->Record);
                    duplicateCommitResponse = std::make_unique<IEventHandle>(ev->Sender, tabletServer,
                        duplicate.release(), 0, ev->Cookie);
                    firstCommitResponse = std::make_unique<IEventHandle>(ev->Sender, tabletServer,
                        result.release(), 0, ev->Cookie);
                    return false;
                }
                response = std::move(result);
            } else {
                return true;
            }
            runtime.Send(new IEventHandle(ev->Sender, tabletServer, response.release(), 0, ev->Cookie), nodeId);
            return false;
        };
        fixture.Start();
        auto first = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        first->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(false);
        InvokeKeeper(runtime, fixture.Keepers.front(), std::move(first));
        UNIT_ASSERT(SimUntil(runtime, [&] { return delayPropose ? bool(firstProposeResponse) : bool(firstCommitResponse); }));
        auto second = fixture.MakeReplaceRequest(1, "mirror-3-dc");
        second->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(delayPropose);
        InvokeKeeper(runtime, fixture.Keepers.front(), std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(validations, delayPropose ? 1 : 2);
        UNIT_ASSERT_VALUES_EQUAL(proposes, 1);
        UNIT_ASSERT_VALUES_EQUAL(commits, delayPropose ? 0 : 1);
        if (delayPropose) {
            runtime.Send(firstProposeResponse.release(), 1);
            UNIT_ASSERT(SimUntil(runtime, [&] { return bool(firstCommitResponse); }));
        }
        UNIT_ASSERT_VALUES_EQUAL(publishedYaml, fixture.MakeReplacementYaml(0, "mirror-3-dc"));
        runtime.Send(firstCommitResponse.release(), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return bool(secondProposeResponse); }),
            "The latest committed config was not proposed after the old Console commit completed");
        runtime.Send(duplicateCommitResponse.release(), 1);
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(1));
        runtime.Send(secondProposeResponse.release(), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return commits == 2; }));
        UNIT_ASSERT_VALUES_EQUAL(publishedYaml, fixture.MakeReplacementYaml(1, "mirror-3-dc"));
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 3);
        CheckDurableConfigConverges(fixture, winner);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(proposes, 2);
        UNIT_ASSERT_VALUES_EQUAL(commits, 2);
    }

    Y_UNIT_TEST(ConsolePublishesLatestConfigAfterDelayedCommit) {
        CheckConsolePublishesLatestConfig(false);
    }

    Y_UNIT_TEST(ConsolePublishesLatestConfigAfterDelayedPropose) {
        CheckConsolePublishesLatestConfig(true);
    }

    void CheckDisconnectPreservesOtherAddedNodeResponses(ui32 disconnectedNodeId) {
        TCommitConfirmationFixture fixture(false, 18);
        fixture.UseMirror3dcGroup(9);
        fixture.Config.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (const auto& node : fixture.Config.GetAllNodes()) {
            fixture.Metadata(node.GetNodeId()).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        auto expanded = MakeStorageConfig(18, 3, {1, 2, 3}, TBlobStorageGroupType::ErasureMirror3dc);
        expanded.MutableSelfManagementConfig()->CopyFrom(fixture.Config.GetSelfManagementConfig());
        for (auto& node : *expanded.MutableAllNodes()) {
            node.MutableLocation()->CopyFrom(fixture.Config.GetAllNodes((node.GetNodeId() - 1) % 9).GetLocation());
        }
        expanded.MutablePrevConfig()->CopyFrom(fixture.Config);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&expanded);
        const std::vector<ui32> oldNodes{1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::vector<ui32> healthyNodes;
        for (ui32 nodeId = 1; nodeId <= 18; ++nodeId) {
            if (nodeId != disconnectedNodeId) {
                healthyNodes.push_back(nodeId);
            }
        }
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, oldNodes));
        auto healthyOldNodes = oldNodes;
        std::erase(healthyOldNodes, disconnectedNodeId);
        UNIT_ASSERT(HasTestConfigQuorum(fixture.Config, healthyOldNodes));
        UNIT_ASSERT(!HasTestConfigQuorum(expanded, oldNodes));
        UNIT_ASSERT(HasTestConfigQuorum(expanded, healthyNodes));

        auto& runtime = fixture.Runtime;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        bool holdNewGathers = true;
        bool sawDisconnect = false;
        THashSet<ui32> targetedNodes;
        THashSet<ui32> proposedNodes;
        THashSet<ui32> gatheredNodes;
        ui32 controllerRequests = 0;
        std::vector<std::unique_ptr<IEventHandle>> delayedGathers;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTabletPipe::EvSend) {
                controllerRequests += ev->Type == TEvBlobStorage::TEvControllerConfigRequest::EventType
                    || ev->Type == TEvBlobStorage::TEvControllerDistconfRequest::EventType;
            }
            if (nodeId >= 10 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (record.GetTargeted() && record.HasProposeStorageConfig()) {
                    targetedNodes.insert(nodeId);
                }
            }
            if (nodeId >= 10 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                    && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record
                        .GetProposedStorageConfig().GetGeneration() == 3) {
                proposedNodes.insert(nodeId);
            }
            if (HandleReplaceEvent(runtime, nodeId, *ev, edge, response)) {
                return false;
            }
            if (!fixture.Keepers.empty() && ev->GetRecipientRewrite() == fixture.Keepers.front()
                    && ev->GetTypeRewrite() == TEvInterconnect::TEvNodeDisconnected::EventType
                    && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == disconnectedNodeId) {
                sawDisconnect = true;
            }
            if (holdNewGathers && nodeId == 1 && ev->Sender.NodeId() >= 10
                    && ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType
                    && ev->Get<NStorage::TEvNodeConfigGather>()->Record.HasProposeStorageConfig()) {
                gatheredNodes.insert(ev->Sender.NodeId());
                delayedGathers.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.Start(true);
        auto base = expanded;
        base.SetGeneration(0);
        base.ClearPrevConfig();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&base);
        const TString yaml = fixture.MakeReplacementYaml(expanded, 0, "mirror-3-dc");
        for (ui32 nodeId = 10; nodeId <= 18; ++nodeId) {
            fixture.Keepers.push_back(RegisterKeeper(runtime, base, nodeId, fixture.State, yaml));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return !runtime.HasImmediateEvents(); }, TDuration::Seconds(1)));
        for (ui32 nodeId = 10; nodeId <= 18; ++nodeId) {
            UNIT_ASSERT(!fixture.Metadata(nodeId).HasCommittedStorageConfig());
        }
        edge = runtime.AllocateEdgeActor(1);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableReplaceStorageConfig()->SetYAML(yaml);
        request->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(true);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
        const bool proposed = SimUntil(runtime, [&] { return gatheredNodes.size() == 9 || response; });
        UNIT_ASSERT_C(proposed && gatheredNodes.size() == 9,
            "the new nodes did not persist and answer the targeted proposal: controllerRequests=" << controllerRequests
            << " targeted=" << targetedNodes.size() << " persisted=" << proposedNodes.size()
            << " gathered=" << gatheredNodes.size() << " response="
            << (response ? response->DebugString() : "pending"));
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(4));
        UNIT_ASSERT(!response);
        runtime.StopNode(disconnectedNodeId);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return sawDisconnect; }, TDuration::Seconds(1)),
            "root did not observe the node disconnection");
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        UNIT_ASSERT_C(!response, "disconnect discarded the pending responses from healthy new nodes");
        holdNewGathers = false;
        ReplayDelayedEvents(runtime, delayedGathers);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return response.has_value(); }), "Replace did not finish");
        UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            response->DebugString());
        const auto committed = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(committed.GetGeneration(), 3);
        UNIT_ASSERT_VALUES_EQUAL(committed.AllNodesSize(), 18);
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(healthyNodes, [&](ui32 nodeId) {
                return fixture.Metadata(nodeId).GetCommittedStorageConfig().GetFingerprint() == committed.GetFingerprint();
            });
        }), "the expansion did not become durable on the healthy nodes");
    }

    Y_UNIT_TEST(OldNodeDisconnectPreservesAddedNodeResponses) {
        CheckDisconnectPreservesOtherAddedNodeResponses(2);
    }

    Y_UNIT_TEST(AddedNodeDisconnectPreservesOtherAddedNodeResponses) {
        CheckDisconnectPreservesOtherAddedNodeResponses(10);
    }

    Y_UNIT_TEST(BridgeBoardFullSelfHealLeavesDisabledSubsystemsUnchanged) {
        TCommitConfirmationFixture fixture(false);
        fixture.Config.MutableSelfManagementConfig()->SetAutomaticStateStorageManagement(false);
        fixture.Config.MutableSelfManagementConfig()->SetAutomaticStateStorageBoardManagement(true);
        fixture.Config.MutableSelfManagementConfig()->SetAutomaticSchemeBoardManagement(false);
        const auto makeConfig = [](auto *config, ui32 nodeId, ui32 actorIdOffset) {
            config->SetSSId(1);
            auto *group = config->MutableRing();
            group->SetNToSelect(1);
            group->SetRingGroupActorIdOffset(actorIdOffset);
            group->AddRing()->AddNode(nodeId);
        };
        makeConfig(fixture.Config.MutableStateStorageConfig(), 1, 7);
        makeConfig(fixture.Config.MutableStateStorageBoardConfig(), 2, 3);
        makeConfig(fixture.Config.MutableSchemeBoardConfig(), 3, 5);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        const auto beforeConversion = fixture.Config;
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        if (const auto error = NStorage::ConvertStorageConfigToBridge(&fixture.Config, bridge)) {
            UNIT_FAIL(*error);
        }
        fixture.Config.SetGeneration(beforeConversion.GetGeneration() + 1);
        fixture.Config.MutablePrevConfig()->CopyFrom(beforeConversion);
        if (const auto error = NStorage::TDistributedConfigKeeper::TransformConfigBeforeCommit(&fixture.Config)) {
            UNIT_FAIL(*error);
        }
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        if (const auto error = NStorage::ValidateConfigUpdate(beforeConversion, fixture.Config)) {
            UNIT_FAIL(*error);
        }
        UNIT_ASSERT(!fixture.Config.GetStateStorageConfig().HasRing());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Config.GetStateStorageConfig().RingGroupsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Config.GetStateStorageConfig().GetRingGroups(0).GetRingGroupActorIdOffset(), 7);
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        const TString originalStateStorage = fixture.Config.GetStateStorageConfig().SerializeAsString();
        const TString originalSchemeBoard = fixture.Config.GetSchemeBoardConfig().SerializeAsString();
        const auto& policy = fixture.Config.GetSelfManagementConfig();
        ui32 reconfigurationRequests = 0;
        TActorId edge;
        std::vector<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> results;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && ev->GetRecipientRewrite() == edge
                    && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                UNIT_ASSERT_C(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                    result.DebugString());
                results.push_back(result);
                return false;
            }
            if (!fixture.Keepers.empty()
                    && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& command = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (command.HasReconfigStateStorage()) {
                    if (ev->Recipient == MakeBlobStorageNodeWardenID(1)) {
                        ev->Rewrite(NStorage::TEvNodeConfigInvokeOnRoot::EventType, fixture.Keepers.front());
                    }
                    const auto& config = command.GetReconfigStateStorage();
                    UNIT_ASSERT_C(!config.HasStateStorageConfig(), config.DebugString());
                    UNIT_ASSERT_C(!config.HasSchemeBoardConfig(), config.DebugString());
                    UNIT_ASSERT_C(config.HasStateStorageBoardConfig(), config.DebugString());
                    ++reconfigurationRequests;
                }
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record;
                for (const auto *config : {&record.GetCommittedStorageConfig(), &record.GetProposedStorageConfig()}) {
                    if (config->GetGeneration()) {
                        UNIT_ASSERT_VALUES_EQUAL(config->GetStateStorageConfig().SerializeAsString(), originalStateStorage);
                        UNIT_ASSERT_VALUES_EQUAL(config->GetSchemeBoardConfig().SerializeAsString(), originalSchemeBoard);
                    }
                }
            }
            return true;
        };
        runtime.Start();
        for (ui32 nodeId : {1, 2, 3}) {
            fixture.Keepers.push_back(RegisterKeeper(runtime, fixture.Config, nodeId, fixture.State, {}, &bridge, &policy));
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, fixture.Keepers) == 1; }));
        edge = runtime.AllocateEdgeActor(1);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto *command = request->Record.MutableSelfHealNodesStateUpdate();
        command->SetEnableSelfHealStateStorage(true);
        command->SetWaitForConfigStep(1);
        command->SetPileupReplicas(true);
        command->SetOverrideRingsCount(3);
        command->SetOverrideReplicasInRingCount(1);
        for (ui32 nodeId : {1, 2, 3}) {
            auto *nodeState = command->AddNodesState();
            nodeState->SetNodeId(nodeId);
            nodeState->SetState(0);
        }
        runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return results.size() == 2; }), "Board self-heal did not complete");
        UNIT_ASSERT_VALUES_EQUAL(reconfigurationRequests, 4);
        const auto committed = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(committed.GetStateStorageConfig().SerializeAsString(), originalStateStorage);
        UNIT_ASSERT_VALUES_EQUAL(committed.GetSchemeBoardConfig().SerializeAsString(), originalSchemeBoard);
        UNIT_ASSERT(!committed.GetStateStorageBoardConfig().HasRing());
        UNIT_ASSERT_VALUES_EQUAL(committed.GetStateStorageBoardConfig().RingGroupsSize(), 1);
        const auto& boardGroup = committed.GetStateStorageBoardConfig().GetRingGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(boardGroup.GetNToSelect(), 3);
        UNIT_ASSERT_VALUES_EQUAL(boardGroup.RingSize(), 3);
        UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&boardGroup,
            &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId), TBridgePileId::FromPileIndex(0));
        CheckDurableConfigConverges(fixture, committed);
    }

    void CheckBridgeFreshNodeExpansion(bool crossPile, bool failProposal, bool changePileList = false) {
        TCommitConfirmationFixture fixture(false, 5);
        fixture.Config = MakeTwoPileBridgedConfig("pile0", "pile1");
        auto *secondHost = fixture.Config.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
        secondHost->SetHostConfigId(2);
        secondHost->SetEnforcedNodeId(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        fixture.State.Metadata.clear();
        fixture.State.RequiredRootNodeId = 1;
        NStorage::EnumerateConfigDrives(fixture.Config, 0, [&](const auto& node, const auto& drive) {
            fixture.State.Metadata[std::pair<ui32, TString>{node.GetNodeId(), drive.GetPath()}]
                .MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        });
        auto& runtime = fixture.Runtime;
        auto& bridge = runtime.GetAppDataBridgeConfig();
        bridge.AddPiles()->SetName("pile0");
        bridge.AddPiles()->SetName("pile1");

        auto expanded = fixture.Config;
        auto *node = expanded.AddAllNodes();
        node->CopyFrom(MakeStorageConfig(5).GetAllNodes(4));
        node->MutableLocation()->SetBridgePileName(crossPile ? "pile1" : "pile0");
        node->MutableLocation()->SetDataCenter("dc0");
        auto *hostConfig = expanded.MutableBlobStorageConfig()->AddDefineHostConfig();
        hostConfig->SetHostConfigId(5);
        for (ui32 index = 1; index <= (crossPile ? 3u : 5u); ++index) {
            auto *drive = hostConfig->AddDrive();
            drive->SetPath("/dev/disk5_" + std::to_string(index));
            drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
        }
        auto *host = expanded.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
        host->SetHostConfigId(5);
        host->SetEnforcedNodeId(5);
        if (changePileList) {
            auto nextBridge = bridge;
            nextBridge.AddPiles()->SetName("pile2");
            bool changed = false;
            if (const auto error = NStorage::UpdatePileRegistry(nextBridge, &expanded, &bridge, &bridge, &changed)) {
                UNIT_FAIL(*error);
            }
            UNIT_ASSERT(changed);
        }
        expanded.SetGeneration(2);
        expanded.MutablePrevConfig()->CopyFrom(fixture.Config);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&expanded);
        auto fresh = expanded;
        fresh.SetGeneration(0);
        fresh.ClearPrevConfig();
        fresh.MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(changePileList ? 3 : 2));
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fresh);
        const TString yaml = fixture.MakeReplacementYaml(expanded, 0, "none")
            + "  bridge_config:\n    piles:\n    - {name: pile0}\n    - {name: pile1}\n"
            + (changePileList ? "    - {name: pile2}\n" : "");

        std::array<TActorId, 6> checkers;
        THashMap<ui32, std::shared_ptr<const NKikimrBlobStorage::TStorageConfig>> configs;
        configs[1] = std::make_shared<const NKikimrBlobStorage::TStorageConfig>(fixture.Config);
        configs[5] = std::make_shared<const NKikimrBlobStorage::TStorageConfig>(fresh);
        bool holdTarget = true;
        std::vector<std::unique_ptr<IEventHandle>> targeted;
        TActorId replaceEdge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> replaceResponse;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HandleReplaceEvent(runtime, nodeId, *ev, replaceEdge, replaceResponse)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvNodeWardenQueryStorageConfig::EventType && checkers[nodeId]
                && ev->Sender == checkers[nodeId]) {
                const auto& config = configs.at(nodeId);
                const auto info = NStorage::TDistributedConfigKeeper::GenerateBridgeInfo(*config,
                    *MakeNodeWardenConfig(*config, &bridge), nodeId);
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvNodeWardenStorageConfig(config, true, info)), nodeId);
                return false;
            }
            if (type == TEvNodeWardenStorageConfig::EventType
                && ev->Recipient == MakeBlobStorageNodeWardenID(nodeId) && checkers[nodeId]) {
                const auto& msg = *ev->Get<TEvNodeWardenStorageConfig>();
                configs[nodeId] = msg.Config;
                runtime.Send(new IEventHandle(checkers[nodeId], ev->Sender, new TEvNodeWardenStorageConfig(
                    msg.Config, msg.SelfManagementEnabled, msg.BridgeInfo, msg.CommittedConfig)), nodeId);
            }
            if (type == NStorage::TEvNodeWardenUpdateConfigFromPeer::EventType && checkers[nodeId]) {
                ev->Rewrite(type, fixture.Keepers[nodeId - 1]);
            }
            if (holdTarget && type == NStorage::TEvNodeConfigScatter::EventType && nodeId == 5
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.GetTargeted()) {
                targeted.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.Start(false, &bridge);
        fixture.Keepers.push_back(RegisterKeeper(runtime, fresh, 5, fixture.State, yaml, &bridge));
        for (ui32 nodeId : {1u, 5u}) {
            const ui32 pileIndex = nodeId == 5 && crossPile ? 1 : 0;
            checkers[nodeId] = runtime.Register(CreateDistconfBridgeConnectionCheckerActor(
                TBridgePileId::FromPileIndex(pileIndex), bridge.GetPiles(pileIndex).GetName()), nodeId);
            runtime.RegisterService(MakeDistconfBridgeConnectionCheckerActorId(), checkers[nodeId]);
        }
        auto prepare = [&](ui32 nodeId, ui32 peerNodeId) {
            const TActorId edge = runtime.AllocateEdgeActor(nodeId);
            runtime.Send(new IEventHandle(checkers[nodeId], edge,
                new TEvInterconnect::TEvPrepareOutgoingConnection(peerNodeId)), nodeId);
            auto response = runtime.WaitForEdgeActorEvent<TEvInterconnect::TEvPrepareOutgoingConnectionResult>(edge);
            if (const auto *error = std::get_if<TString>(&response->Get()->Conclusion)) {
                UNIT_FAIL(*error);
            }
            return std::get<THashMap<TString, TString>>(response->Get()->Conclusion);
        };
        auto check = [&](ui32 nodeId, ui32 peerNodeId, THashMap<TString, TString> params) {
            const TActorId edge = runtime.AllocateEdgeActor(nodeId);
            runtime.Send(new IEventHandle(checkers[nodeId], edge,
                new TEvInterconnect::TEvCheckIncomingConnection(peerNodeId, std::move(params))), nodeId);
            auto response = runtime.WaitForEdgeActorEvent<TEvInterconnect::TEvCheckIncomingConnectionResult>(edge);
            return response->Get()->ErrorReason;
        };
        auto storageConfig = [](const THashMap<TString, TString>& params) {
            UNIT_ASSERT_VALUES_EQUAL(params.size(), 1);
            NKikimrBlobStorage::TConnectivityPayload payload;
            UNIT_ASSERT(payload.ParseFromString(params.begin()->second));
            return payload.GetStorageConfig();
        };

        replaceEdge = runtime.AllocateEdgeActor(1);
        auto replace = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        replace->Record.MutableReplaceStorageConfig()->SetYAML(yaml);
        replace->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(true);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, replace.release()), 1);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return !targeted.empty() || replaceResponse; }),
            "Replace did not reach the added-node proposal");
        UNIT_ASSERT_C(!replaceResponse, (replaceResponse ? replaceResponse->DebugString() : ""));
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(1));
        const auto rootParams = prepare(1, 5);
        UNIT_ASSERT_VALUES_EQUAL(storageConfig(rootParams).GetGeneration(), 1);
        THashMap<TString, TString> olderParams;
        if (changePileList) {
            UNIT_ASSERT(rootParams.contains("distconf-pile-list-changed"));
            UNIT_ASSERT(!storageConfig(rootParams).GetClusterStateDetails().GetPileRegistry().GetPileListChanged());
            NKikimrBlobStorage::TConnectivityPayload older;
            TBridgePileId::FromPileIndex(1).CopyToProto(&older,
                &NKikimrBlobStorage::TConnectivityPayload::SetBridgePileId);
            older.MutableStorageConfig()->CopyFrom(fixture.Config);
            UNIT_ASSERT(older.SerializeToString(&olderParams["distconf"]));
            const auto error = check(1, 2, olderParams);
            UNIT_ASSERT(error);
            UNIT_ASSERT_STRING_CONTAINS(*error, "has to be upgraded");
        }
        const auto freshParams = prepare(5, 1);
        UNIT_ASSERT_VALUES_EQUAL(storageConfig(freshParams).GetGeneration(), 0);
        const auto rootError = check(1, 5, freshParams);
        UNIT_ASSERT_C(!rootError, rootError.value_or(""));
        const auto freshError = check(5, 1, rootParams);
        UNIT_ASSERT_C(!freshError, freshError.value_or(""));
        NKikimrBlobStorage::TConnectivityPayload formatted;
        UNIT_ASSERT(formatted.ParseFromString(freshParams.begin()->second));
        TBridgePileId::FromPileIndex(1).CopyToProto(&formatted,
            &NKikimrBlobStorage::TConnectivityPayload::SetBridgePileId);
        formatted.SetBridgePileName("pile1");
        THashMap<TString, TString> formattedParams;
        UNIT_ASSERT(formatted.SerializeToString(&formattedParams[freshParams.begin()->first]));
        const auto formattedError = check(1, 2, std::move(formattedParams));
        UNIT_ASSERT(formattedError);
        UNIT_ASSERT_STRING_CONTAINS(*formattedError, "peer cluster state is obsolete");
        SimUntil(runtime, [] { return false; }, TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(configs.at(5)->GetGeneration(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.NodeWardenConfigs.at(5)->GetGeneration(), 0);

        if (failProposal) {
            targeted.clear();
            UNIT_ASSERT(SimUntil(runtime, [&] { return bool(replaceResponse); }, TDuration::Seconds(25)));
            UNIT_ASSERT_C(replaceResponse->GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                replaceResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig().GetGeneration(), 1);
            const auto afterAbort = prepare(1, 5);
            UNIT_ASSERT_VALUES_EQUAL(storageConfig(afterAbort).GetGeneration(), 1);
            if (changePileList) {
                UNIT_ASSERT(afterAbort.contains("distconf"));
                UNIT_ASSERT(!check(1, 2, olderParams));
            }
            UNIT_ASSERT_VALUES_EQUAL(configs.at(5)->GetGeneration(), 0);
            UNIT_ASSERT(!fixture.Metadata(5).HasCommittedStorageConfig());
            return;
        }
        holdTarget = false;
        ReplayDelayedEvents(runtime, targeted);
        UNIT_ASSERT(SimUntil(runtime, [&] { return bool(replaceResponse); }, TDuration::Seconds(25)));
        UNIT_ASSERT_C(replaceResponse->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            replaceResponse->DebugString());
        const auto committedParams = prepare(1, 5);
        UNIT_ASSERT_VALUES_EQUAL(storageConfig(committedParams).GetGeneration(), 2);
        if (changePileList) {
            UNIT_ASSERT(committedParams.contains("distconf-pile-list-changed"));
            const auto error = check(1, 2, olderParams);
            UNIT_ASSERT(error);
            UNIT_ASSERT_STRING_CONTAINS(*error, "has to be upgraded");
        }
        check(5, 1, committedParams);
        UNIT_ASSERT(SimUntil(runtime, [&] { return configs.at(5)->GetGeneration() == 2; }));
        UNIT_ASSERT(!check(1, 5, prepare(5, 1)));
        UNIT_ASSERT(!check(5, 1, prepare(1, 5)));
        UNIT_ASSERT(SimUntil(runtime, [&] {
            return fixture.Metadata(5).GetCommittedStorageConfig().GetGeneration() == 2;
        }));
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig().AllNodesSize(), 5);
    }

    Y_UNIT_TEST(BridgeFreshSamePileExpansion) {
        CheckBridgeFreshNodeExpansion(false, false);
    }

    Y_UNIT_TEST(BridgeFreshCrossPileExpansion) {
        CheckBridgeFreshNodeExpansion(true, false);
    }

    Y_UNIT_TEST(BridgeFailedSamePileExpansionNotPublished) {
        CheckBridgeFreshNodeExpansion(false, true);
    }

    Y_UNIT_TEST(BridgeFailedCrossPileExpansionNotPublished) {
        CheckBridgeFreshNodeExpansion(true, true);
    }

    Y_UNIT_TEST(BridgePendingFeatureGateClearedAfterFailedExpansion) {
        CheckBridgeFreshNodeExpansion(true, true, true);
    }

    Y_UNIT_TEST(BridgePendingFeatureGateRetainedAfterCommittedExpansion) {
        CheckBridgeFreshNodeExpansion(true, false, true);
    }

    enum class EConfigValidationWait {
        ControllerInterface,
        ControllerConfig,
        ConsoleConfig,
    };

    void CheckChangedReplaceValidation(EConfigValidationWait wait, bool slowResponses = false) {
        const ui32 nodeCount = slowResponses ? 20 : 3;
        TCommitConfirmationFixture fixture(true, nodeCount);
        fixture.UseMirror3dcGroup(nodeCount);
        fixture.State.RequiredRootNodeId = 1;
        if (slowResponses) {
            UNIT_ASSERT(wait == EConfigValidationWait::ConsoleConfig);
            for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
                fixture.State.RequiredParentNodeIds[nodeId] = 2;
            }
        }
        if (wait == EConfigValidationWait::ConsoleConfig) {
            auto *stateStorage = fixture.Config.MutableStateStorageConfig();
            stateStorage->SetSSId(1);
            stateStorage->MutableRing()->SetNToSelect(1);
            stateStorage->MutableRing()->AddNode(1);
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
            for (const auto& node : fixture.Config.GetAllNodes()) {
                fixture.Metadata(node.GetNodeId()).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
            }
        }
        auto& runtime = fixture.Runtime;
        TActorId tabletServer;
        TActorId replaceEdge;
        TActorId queryEdge;
        TActorId nextEdge;
        bool holdValidation = true;
        bool controllerConnected = false;
        bool consoleConnected = false;
        ui32 firstResponseCount = 0;
        ui32 proposedWrites = 0;
        ui32 slowReplies = 0;
        THashMap<ui32, ui32> refillRequests;
        THashMap<ui32, THashSet<TActorId>> refillForwarders;
        THashMap<TActorId, ui32> refillOrigins;
        THashMap<ui32, ui32> fullRefillReplies;
        std::vector<std::unique_ptr<IEventHandle>> delayedResponses;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> replaceResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> queryResponse;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> nextResponse;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (!tabletServer) {
                tabletServer = runtime.Register(new TIgnoreActor, 1);
                runtime.RegisterService(MakeTabletResolverID(), runtime.Register(new TIgnoreActor, 1));
            }
            if (replaceEdge && CaptureInvokeResult(*ev, replaceEdge, replaceResponse)) {
                ++firstResponseCount;
                return false;
            }
            if ((queryEdge && CaptureInvokeResult(*ev, queryEdge, queryResponse))
                || (nextEdge && CaptureInvokeResult(*ev, nextEdge, nextResponse))) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (slowResponses && type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->GetRecipientRewrite() == fixture.Keepers[nodeId - 1]) {
                const auto& query = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.GetQueryConfig();
                const ui32 origin = query.GetRefillNodeId();
                if (origin && nodeId != origin) {
                    refillForwarders[origin].insert(ev->Sender);
                    if (nodeId == 1) {
                        ++refillRequests[origin];
                        refillOrigins[ev->Sender] = origin;
                    }
                }
            }
            if (slowResponses && nodeId == 1
                && ev->Type == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                if (const auto it = refillOrigins.find(ev->Recipient); it != refillOrigins.end()) {
                    const auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                    if (result.HasQueryConfig() && result.GetQueryConfig().GetFresh()) {
                        ++fullRefillReplies[it->second];
                    }
                }
            }
            if (type == TEvTabletResolver::TEvForward::EventType) {
                const ui64 tabletId = ev->Get<TEvTabletResolver::TEvForward>()->TabletID;
                if (tabletId == MakeBSControllerID() || tabletId == MakeConsoleID()) {
                    runtime.Send(new IEventHandle(ev->Sender, tabletServer,
                        new TEvTabletResolver::TEvForwardResult(tabletId, tabletServer, tabletServer, 0)), nodeId);
                    return false;
                }
            } else if (type == TEvTabletPipe::TEvConnect::EventType
                && ev->GetRecipientRewrite() == tabletServer) {
                const ui64 tabletId = ev->Get<TEvTabletPipe::TEvConnect>()->Record.GetTabletId();
                runtime.Send(new IEventHandle(ev->Sender, tabletServer, new TEvTabletPipe::TEvConnectResult(
                    NKikimrProto::OK, tabletId, ev->Sender, tabletServer, true, 1, TString()), 0, ev->Cookie), nodeId);
                return false;
            } else if (type == TEvTabletPipe::TEvClientConnected::EventType) {
                const auto& msg = *ev->Get<TEvTabletPipe::TEvClientConnected>();
                UNIT_ASSERT_VALUES_EQUAL(msg.Status, NKikimrProto::OK);
                controllerConnected |= msg.TabletId == MakeBSControllerID();
                consoleConnected |= msg.TabletId == MakeConsoleID();
            } else if (type == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record
                    .GetProposedStorageConfig().GetGeneration() == 2) {
                ++proposedWrites;
            }
            if (type != TEvTabletPipe::EvSend) {
                return true;
            }
            std::unique_ptr<IEventBase> response;
            bool blockResponse = false;
            bool delayResponse = false;
            if (ev->Type == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerConfigResponse>();
                result->Record.MutableResponse()->AddStatus()->SetInterfaceVersion(BSC_INTERFACE_DISTCONF_CONTROL);
                response = std::move(result);
                blockResponse = wait == EConfigValidationWait::ControllerInterface;
                delayResponse = !blockResponse;
            } else if (ev->Type == TEvBlobStorage::TEvControllerDistconfRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerDistconfResponse>();
                result->Record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                response = std::move(result);
                blockResponse = wait == EConfigValidationWait::ControllerConfig;
                delayResponse = wait == EConfigValidationWait::ConsoleConfig;
            } else if (ev->Type == TEvBlobStorage::TEvControllerProposeConfigRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigResponse>();
                result->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded);
                response = std::move(result);
            } else if (ev->Type == TEvBlobStorage::TEvControllerValidateConfigRequest::EventType) {
                auto result = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
                result->Record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
                result->Record.SetYAML(fixture.MakeReplacementYaml(1, "mirror-3-dc"));
                response = std::move(result);
                blockResponse = true;
            } else {
                return true;
            }
            auto reply = std::make_unique<IEventHandle>(ev->Sender, tabletServer, response.release(), 0, ev->Cookie);
            if (slowResponses && ev->Type != TEvBlobStorage::TEvControllerProposeConfigRequest::EventType) {
                ++slowReplies;
                runtime.Schedule(runtime.GetClock() + TDuration::Minutes(3), reply.release(), nullptr, nodeId);
            } else if (holdValidation && blockResponse) {
                delayedResponses.push_back(std::move(reply));
            } else if (holdValidation && delayResponse) {
                runtime.Schedule(runtime.GetClock() + TDuration::Seconds(10), reply.release(), nullptr, nodeId);
            } else {
                runtime.Send(reply.release(), nodeId);
            }
            return false;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        replaceEdge = runtime.AllocateEdgeActor(1);
        const TInstant started = runtime.GetClock();
        auto replace = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        replace->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(wait != EConfigValidationWait::ConsoleConfig);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), replaceEdge, replace.release()), 1);
        if (slowResponses) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return slowReplies == 1; }));
            for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
                fixture.FormatKeeper(nodeId);
            }
        }
        queryEdge = runtime.AllocateEdgeActor(1);
        auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        query->Record.MutableQueryConfig();
        runtime.Send(new IEventHandle(fixture.Keepers.front(), queryEdge, query.release()), 1);
        if (slowResponses) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return replaceResponse || slowReplies == 3; }, TDuration::Minutes(7)));
            UNIT_ASSERT_C(!replaceResponse, "valid slow configuration was rejected: " << replaceResponse->DebugString());
            SimUntil(runtime, [] { return false; }, started + TDuration::Minutes(9) - TDuration::Seconds(1) - runtime.GetClock());
            UNIT_ASSERT(!replaceResponse && !queryResponse);
            UNIT_ASSERT_VALUES_EQUAL(proposedWrites, 0);
            for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
                UNIT_ASSERT_C(refillRequests[nodeId] >= 10, "refill retries were not exercised: " << nodeId);
                ui32 liveForwarders = 0;
                for (const TActorId actorId : refillForwarders[nodeId]) {
                    liveForwarders += runtime.GetActor(actorId) != nullptr;
                }
                UNIT_ASSERT_C(liveForwarders <= 2, "refill forwarding requests accumulated: node# " << nodeId
                    << " live# " << liveForwarders << " attempts# " << refillRequests[nodeId]);
                UNIT_ASSERT_VALUES_EQUAL(fullRefillReplies[nodeId], 0);
            }
            UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
            UNIT_ASSERT(SimUntil(runtime, [&] { return replaceResponse && queryResponse; }, TDuration::Seconds(35)));
            using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
            UNIT_ASSERT_C(replaceResponse->GetStatus() == TResult::OK, replaceResponse->DebugString());
            UNIT_ASSERT_C(queryResponse->GetStatus() == TResult::OK, queryResponse->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(queryResponse->GetQueryConfig().GetConfig().GetGeneration(), 2);
            UNIT_ASSERT(controllerConnected && consoleConnected);
            UNIT_ASSERT_VALUES_EQUAL(slowReplies, 3);
            UNIT_ASSERT_VALUES_EQUAL(firstResponseCount, 1);
            const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
            UNIT_ASSERT(SimUntil(runtime, [&] {
                for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
                    const auto it = fixture.State.NodeWardenConfigs.find(nodeId);
                    if (it == fixture.State.NodeWardenConfigs.end()
                        || it->second->GetFingerprint() != winner.GetFingerprint()) {
                        return false;
                    }
                }
                return true;
            }));
            for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
                UNIT_ASSERT_VALUES_EQUAL(fullRefillReplies[nodeId], 1);
                for (const TActorId actorId : refillForwarders[nodeId]) {
                    UNIT_ASSERT(!runtime.GetActor(actorId));
                }
            }
            CheckDurableConfigConverges(fixture, winner);
            return;
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return !delayedResponses.empty(); }, TDuration::Seconds(25)));
        UNIT_ASSERT(!replaceResponse && !queryResponse);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return replaceResponse && queryResponse; },
            started + TDuration::Minutes(10) + TDuration::Seconds(5) - runtime.GetClock()), "validation blocked the configuration queue");
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        UNIT_ASSERT_C(replaceResponse->GetStatus() == TResult::ERROR, replaceResponse->DebugString());
        UNIT_ASSERT_C(TString(replaceResponse->GetErrorReason()).Contains("Timed out waiting for BSC or Console"),
            replaceResponse->DebugString());
        UNIT_ASSERT_C(queryResponse->GetStatus() == TResult::OK, queryResponse->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(queryResponse->GetQueryConfig().GetConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proposedWrites, 0);
        UNIT_ASSERT(controllerConnected);
        UNIT_ASSERT(wait != EConfigValidationWait::ConsoleConfig || consoleConnected);
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        holdValidation = false;
        ReplayDelayedEvents(runtime, delayedResponses);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(firstResponseCount, 1);
        nextEdge = runtime.AllocateEdgeActor(1);
        auto next = fixture.MakeReplaceRequest(0, "mirror-3-dc");
        next->Record.MutableReplaceStorageConfig()->SetSkipConsoleValidation(wait != EConfigValidationWait::ConsoleConfig);
        runtime.Send(new IEventHandle(fixture.Keepers.front(), nextEdge, next.release()), 1);
        UNIT_ASSERT(SimUntil(runtime, [&] { return nextResponse.has_value(); }, TDuration::Seconds(35)));
        UNIT_ASSERT_C(nextResponse->GetStatus() == TResult::OK, nextResponse->DebugString());
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        CheckDurableConfigConverges(fixture, winner);
        UNIT_ASSERT_VALUES_EQUAL(firstResponseCount, 1);
    }

    Y_UNIT_TEST(ReplaceBscInterfaceTimeout) {
        CheckChangedReplaceValidation(EConfigValidationWait::ControllerInterface);
    }

    Y_UNIT_TEST(ReplaceBscValidationTimeout) {
        CheckChangedReplaceValidation(EConfigValidationWait::ControllerConfig);
    }

    Y_UNIT_TEST(ReplaceConsoleValidationTimeout) {
        CheckChangedReplaceValidation(EConfigValidationWait::ConsoleConfig);
    }

    Y_UNIT_TEST(RefillBoundedDuringSlowValidation) {
        CheckChangedReplaceValidation(EConfigValidationWait::ConsoleConfig, true);
    }

    Y_UNIT_TEST(ReplaceConfirmsCommitAfterPipeClose) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& [config, state, runtime, keepers] = fixture;
        const TActorId edge = runtime.AllocateEdgeActor(1);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        TActorId handlerId;
        TActorId controllerPipeId;
        bool allowSecondCommit = false;
        std::vector<std::unique_ptr<IEventHandle>> pendingCommits;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            if (ev->GetTypeRewrite() == TEvTabletPipe::EvSend) {
                if (ev->Type == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                    handlerId = ev->Sender;
                    controllerPipeId = ev->GetRecipientRewrite();
                }
            }
            if (ReplyToReplaceController(runtime, nodeId, *ev, edge)) {
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType
                && ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record.GetCommittedStorageConfig().GetGeneration() == 2
                && (nodeId == 3 || (nodeId == 2 && !allowSecondCommit))) {
                pendingCommits.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        auto request = fixture.MakeReplaceRequest();
        runtime.Send(new IEventHandle(keepers.front(), edge, request.release()), 1);

        UNIT_ASSERT_C(!SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(2)),
                      (response ? response->DebugString() : "replace completed before committed quorum"));
        UNIT_ASSERT_VALUES_EQUAL(pendingCommits.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(1).GetCommittedStorageConfig().GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetProposedStorageConfig().GetGeneration(), 2);

        runtime.Send(new IEventHandle(handlerId, controllerPipeId, new TEvTabletPipe::TEvClientDestroyed(
            MakeBSControllerID(), controllerPipeId, {})), 1);
        runtime.Send(new IEventHandle(handlerId, controllerPipeId, new TEvTabletPipe::TEvClientConnected(
            MakeBSControllerID(), NKikimrProto::ERROR, controllerPipeId, {}, false, false, 0)), 1);
        UNIT_ASSERT(!SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(1)));

        allowSecondCommit = true;
        for (auto& ev : pendingCommits) {
            if (ev->GetRecipientRewrite().NodeId() == 2) {
                runtime.Send(ev.release(), 2);
            }
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(5)));
        UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                      response->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(2).GetCommittedStorageConfig().GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(3).GetCommittedStorageConfig().GetGeneration(), 1);
        UNIT_ASSERT(QueryKeeper(runtime, keepers.front()).HasScepter());
    }

    void CheckAmbiguousRecovery(bool withNewerConfig) {
        TCommitConfirmationFixture fixture;
        fixture.Start();
        auto& [config, state, runtime, keepers] = fixture;

        NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs collected;
        auto *nodes = collected.AddNodes();
        nodes->MutableBaseConfig()->CopyFrom(config);
        for (const auto& node : config.GetAllNodes()) {
            nodes->AddNodeIds()->CopyFrom(node);
        }
        auto first = config;
        first.SetGeneration(2);
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&first);
        auto second = first;
        second.SetSelfAssemblyUUID("conflicting-proposal");
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&second);
        AddCollectedConfig(collected, first, {1, 2, 3});
        AddCollectedConfig(collected, second, {1, 2, 3}, false);
        if (withNewerConfig) {
            auto newer = first;
            newer.SetGeneration(3);
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&newer);
            AddCollectedConfig(collected, newer, {1, 2, 3});
        }
        UNIT_ASSERT(runtime.WrapInActorContext(keepers.front(), [&](IActor *actor) {
            auto *keeper = static_cast<NStorage::TDistributedConfigKeeper*>(actor);
            const auto result = keeper->ProcessCollectConfigs(&collected, std::nullopt, true);
            if (withNewerConfig) {
                UNIT_ASSERT_C(!result.ErrorReason, result.ErrorReason.value_or(""));
            } else {
                UNIT_ASSERT(result.ErrorReason);
                UNIT_ASSERT_VALUES_EQUAL(*result.ErrorReason, "Persistent config quorum with different fingerprints");
            }
        }));
    }

    Y_UNIT_TEST(RecoveryRejectsAmbiguousLatestGeneration) {
        CheckAmbiguousRecovery(false);
    }

    Y_UNIT_TEST(RecoveryIgnoresOlderConflict) {
        CheckAmbiguousRecovery(true);
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
    void CheckDisklessRefillReportsToNodeWarden(bool legacyReply) {
        TCommitConfirmationFixture fixture;
        fixture.Config.MutableBlobStorageConfig()->MutableDefineHostConfig(2)->ClearDrive();
        NStorage::TDistributedConfigKeeper::UpdateFingerprint(&fixture.Config);
        for (ui32 nodeId : {1, 2}) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(fixture.Config);
        }
        fixture.State.Metadata.erase(std::pair<ui32, TString>{3, "/dev/disk3_1"});
        auto& runtime = fixture.Runtime;
        TActorId refillActor;
        bool replyDelayed = false;
        bool replyDelivered = false;
        IEventHandle *delayedReply = nullptr;
        ui32 leafMetadataWrites = 0;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenWriteMetadata::EventType) {
                ++leafMetadataWrites;
            }
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.GetQueryConfig().GetRequireFresh()) {
                refillActor = ev->Sender;
            }
            if (ev.get() == delayedReply) {
                replyDelivered = true;
                delayedReply = nullptr;
            } else if (!replyDelayed && refillActor && ev->GetRecipientRewrite() == refillActor
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                UNIT_ASSERT(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
                UNIT_ASSERT(result.GetQueryConfig().GetFresh());
                if (legacyReply) {
                    result.MutableQueryConfig()->SetFresh(false);
                }
                replyDelayed = true;
                delayedReply = ev.get();
                runtime.Schedule(runtime.GetClock() + TDuration::Seconds(25), ev.release(), nullptr, nodeId);
                return false;
            }
            return true;
        };
        fixture.Start(true);
        UNIT_ASSERT(SimUntil(runtime, [&] { return replyDelayed; }));
        UNIT_ASSERT(fixture.State.NodeWardenConfigs.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.NodeWardenConfigs.at(3)->GetGeneration(), 0);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            const auto& reported = fixture.State.NodeWardenConfigs.at(3);
            return reported->GetFingerprint() == fixture.Config.GetFingerprint();
        }, TDuration::Seconds(28)), "refill applied the configuration without delivering it to NodeWarden");
        UNIT_ASSERT(replyDelivered);
        UNIT_ASSERT_VALUES_EQUAL(leafMetadataWrites, 0);
    }

    Y_UNIT_TEST(DisklessFreshRefillReportsConfig) {
        CheckDisklessRefillReportsToNodeWarden(false);
    }

    Y_UNIT_TEST(DisklessLegacyRefillReportsConfig) {
        CheckDisklessRefillReportsToNodeWarden(true);
    }

    void CheckLateRefillReply(TDuration responseDelay, bool lateError = false) {
        TCommitConfirmationFixture fixture;
        fixture.Metadata(3).Clear();
        auto& runtime = fixture.Runtime;
        ui32 delayedReplies = 0;
        TActorId refillActor;
        THashSet<ui64> delayedCookies;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (nodeId == 3 && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.GetQueryConfig().GetRequireFresh()) {
                refillActor = ev->Sender;
            }
            if (refillActor && ev->GetRecipientRewrite() == refillActor
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (result.HasQueryConfig() && result.GetQueryConfig().GetFresh()
                    && delayedCookies.insert(ev->Cookie).second) {
                    ++delayedReplies;
                    if (lateError && delayedReplies == 1) {
                        result.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE);
                    }
                    runtime.Schedule(runtime.GetClock() + (lateError && delayedReplies != 1
                        ? TDuration::Seconds(15) : responseDelay), ev.release(), nullptr, nodeId);
                    return false;
                }
            }
            return true;
        };
        fixture.Start(true);
        UNIT_ASSERT(SimUntil(runtime, [&] { return delayedReplies != 0; }));
        UNIT_ASSERT(!fixture.Metadata(3).HasCommittedStorageConfig());
        const TInstant firstResponsePrepared = runtime.GetClock();
        if (lateError) {
            SimUntil(runtime, [] { return false; }, TDuration::Seconds(45));
            UNIT_ASSERT_VALUES_EQUAL(delayedReplies, 2);
            UNIT_ASSERT(!fixture.Metadata(3).HasCommittedStorageConfig());
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return fixture.Metadata(3).GetCommittedStorageConfig().GetFingerprint() == fixture.Config.GetFingerprint();
        }, firstResponsePrepared + responseDelay + TDuration::Seconds(lateError ? 10 : 5) - runtime.GetClock()),
            "a valid late refill reply was never installed");
        if (lateError) {
            UNIT_ASSERT_VALUES_EQUAL(delayedReplies, 2);
        }
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers[0], true).HasScepter());
    }

    Y_UNIT_TEST(FreshRefillAcceptsSuccessAfterRequestRetry) {
        CheckLateRefillReply(TDuration::Seconds(40));
    }

    Y_UNIT_TEST(FreshRefillAcceptsSuccessDuringRetryDelay) {
        CheckLateRefillReply(TDuration::MilliSeconds(30'500));
    }

    Y_UNIT_TEST(RefillIgnoresLateError) {
        CheckLateRefillReply(TDuration::Seconds(40), true);
    }

    struct TQueryBatch {
        struct TRequest {
            ui32 NodeId;
            TActorId Edge;
            std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> Result;
            ui32 Count = 0;
            TIntrusivePtr<TEventSerializedData> Buffer;
        };

        TCommitConfirmationFixture& Fixture;
        std::vector<TRequest> Requests;

        TQueryBatch(TCommitConfirmationFixture& fixture, std::initializer_list<ui32> nodeIds)
            : Fixture(fixture)
        {
            for (ui32 nodeId : nodeIds) {
                Requests.push_back({nodeId, Fixture.Runtime.AllocateEdgeActor(nodeId), {}, 0, {}});
            }
        }

        bool Capture(IEventHandle& ev) {
            if (ev.GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                return false;
            }
            for (size_t i = 0; i < Requests.size(); ++i) {
                auto& request = Requests[i];
                if (ev.GetRecipientRewrite() == request.Edge) {
                    UNIT_ASSERT_VALUES_EQUAL(ev.Cookie, 100 + i);
                    request.Buffer = ev.GetChainBuffer();
                    request.Result.emplace(ev.Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record);
                    ++request.Count;
                    return true;
                }
            }
            return false;
        }

        void Send(size_t index, std::unique_ptr<NStorage::TEvNodeConfigInvokeOnRoot> request = {}) {
            const auto& target = Requests[index];
            if (!request) {
                request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
                request->Record.MutableQueryConfig()->SetRequireFresh(true);
                if (target.NodeId != 1) {
                    request->Record.MutableQueryConfig()->SetRefillNodeId(target.NodeId);
                }
            }
            Fixture.Runtime.Send(new IEventHandle(Fixture.Keepers[target.NodeId - 1], target.Edge,
                request.release(), 0, 100 + index), target.NodeId);
        }

        bool AllReplied() const {
            return std::ranges::all_of(Requests, [](const auto& request) { return request.Result.has_value(); });
        }
    };

    void CheckFreshQueryBatching(bool errorWindow) {
        TCommitConfirmationFixture fixture(false, 8);
        fixture.UseMirror3dcGroup(8);
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        fixture.Start(true);
        CheckDurableConfigConverges(fixture, fixture.Config);
        QueryKeeper(runtime, fixture.Keepers[0]);
        TQueryBatch batch(fixture, {
            errorWindow ? 2u : 1u, 1, 1, errorWindow ? 3u : 2u, 1, 1, 1, errorWindow ? 4u : 3u});
        bool holdReads = true;
        bool advanceReceived = false;
        THashSet<ui64> freshRequests;
        THashSet<TActorId> rootReaders;
        std::vector<std::unique_ptr<IEventHandle>> heldReads;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ev->GetRecipientRewrite() == fixture.Keepers[0]
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& request = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (request.GetQueryConfig().GetRequireFresh()) {
                    const ui32 origin = request.GetQueryConfig().GetRefillNodeId();
                    freshRequests.insert(origin ? origin : ev->Cookie);
                }
                advanceReceived |= request.HasAdvanceGeneration();
            }
            if (batch.Capture(*ev)) {
                return false;
            }
            if (nodeId == 1 && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType) {
                rootReaders.insert(ev->Sender);
                if (holdReads) {
                    heldReads.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        Y_DEFER { runtime.FilterFunction = {}; };
        const TInstant started = runtime.GetClock();
        if (errorWindow) {
            UNIT_ASSERT(runtime.WrapInActorContext(fixture.Keepers[0], [&](IActor *actor) {
                static_cast<NStorage::TDistributedConfigKeeper*>(actor)->SwitchToError("transient quorum loss");
            }));
        }
        batch.Send(0);
        UNIT_ASSERT(SimUntil(runtime, [&] { return freshRequests.size() == 1; }, TDuration::MilliSeconds(200)));
        if (!errorWindow) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return !heldReads.empty(); }));
        }
        batch.Send(1);
        batch.Send(2);
        batch.Send(3);
        UNIT_ASSERT(SimUntil(runtime, [&] { return freshRequests.size() == 4; }, TDuration::MilliSeconds(200)));
        auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        advance->Record.MutableAdvanceGeneration();
        batch.Send(4, std::move(advance));
        UNIT_ASSERT(SimUntil(runtime, [&] { return advanceReceived; }, TDuration::MilliSeconds(200)));
        batch.Send(5);
        batch.Send(6);
        batch.Send(7);
        UNIT_ASSERT(SimUntil(runtime, [&] { return freshRequests.size() == 7; }, TDuration::MilliSeconds(200)));
        if (errorWindow) {
            UNIT_ASSERT(runtime.GetClock() - started < TDuration::MilliSeconds(1500));
            UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers[0], true).HasScepter());
            UNIT_ASSERT(rootReaders.empty());
            UNIT_ASSERT(std::ranges::none_of(batch.Requests, [](const auto& request) { return request.Result.has_value(); }));
            UNIT_ASSERT(SimUntil(runtime, [&] { return !heldReads.empty(); }, TDuration::Seconds(5)));
        }
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers[0], true).HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(rootReaders.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(heldReads.size(), 1);
        holdReads = false;
        ReplayDelayedEvents(runtime, heldReads);
        UNIT_ASSERT(SimUntil(runtime, [&] { return batch.AllReplied(); }));
        const auto winner = QueryKeeper(runtime, fixture.Keepers[0]).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(rootReaders.size(), 4);
        UNIT_ASSERT(batch.Requests[1].Buffer && batch.Requests[1].Buffer == batch.Requests[2].Buffer);
        UNIT_ASSERT(batch.Requests[5].Buffer && batch.Requests[5].Buffer == batch.Requests[6].Buffer);
        UNIT_ASSERT(batch.Requests[1].Buffer != batch.Requests[5].Buffer);
        if (!errorWindow) {
            UNIT_ASSERT(batch.Requests[0].Buffer != batch.Requests[1].Buffer);
        }
        for (size_t i = 0; i < batch.Requests.size(); ++i) {
            const auto& request = batch.Requests[i];
            UNIT_ASSERT_C(request.Result->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                request.Result->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(request.Count, 1);
            if (i != 4) {
                UNIT_ASSERT(request.Result->GetQueryConfig().GetFresh());
                UNIT_ASSERT(NStorage::TDistributedConfigKeeper::CheckFingerprint(
                    request.Result->GetQueryConfig().GetConfig()));
                UNIT_ASSERT_VALUES_EQUAL(request.Result->GetQueryConfig().GetConfig().GetGeneration(), i < 4 ? 1 : 2);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(FreshBatchMutationBarrier) {
        CheckFreshQueryBatching(false);
    }

    Y_UNIT_TEST(PendingFreshBatchMutationBarrier) {
        CheckFreshQueryBatching(true);
    }

    void CheckMinimalConfigQuorum(TCommitConfirmationFixture& fixture, ui32 firstNodeId, ui32 lastNodeId) {
        UNIT_ASSERT(firstNodeId <= lastNodeId);
        std::vector<NStorage::TSuccessfulDisk> readable;
        for (ui32 nodeId = firstNodeId; nodeId <= lastNodeId; ++nodeId) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Metadata(nodeId).GetCommittedStorageConfig().GetFingerprint(),
                fixture.Config.GetFingerprint());
            readable.emplace_back(NStorage::TNodeIdentifier(fixture.Config.GetAllNodes(nodeId - 1)),
                "/dev/disk" + std::to_string(nodeId) + "_1", std::make_optional<ui64>(nodeId * 1000 + 1));
        }
        const auto nwConfig = MakeNodeWardenConfig(fixture.Config);
        TStringStream quorumError;
        UNIT_ASSERT_C(NStorage::HasConfigQuorum(fixture.Config, readable,
            TBridgePileId(), *nwConfig, false, &quorumError), quorumError.Str());
        auto insufficient = readable;
        insufficient.pop_back();
        UNIT_ASSERT(!NStorage::HasConfigQuorum(fixture.Config, insufficient,
            TBridgePileId(), *nwConfig, false));
    }

    Y_UNIT_TEST(EmptySubtreeRefillsTogether) {
        constexpr ui32 firstEmptyNode = 13;
        constexpr ui32 nodeCount = 20;
        TCommitConfirmationFixture fixture(true, nodeCount);
        fixture.UseMirror3dcGroup(nodeCount);
        fixture.State.RequiredRootNodeId = 1;
        fixture.State.RequiredParentNodeIds[firstEmptyNode] = 2;
        for (ui32 nodeId = firstEmptyNode + 1; nodeId <= nodeCount; ++nodeId) {
            fixture.State.RequiredParentNodeIds[nodeId] = nodeId - 1;
        }
        auto& runtime = fixture.Runtime;
        fixture.Start(true);
        CheckDurableConfigConverges(fixture, fixture.Config);
        CheckMinimalConfigQuorum(fixture, 2, firstEmptyNode - 1);

        bool holdPublication = true;
        bool holdFresh = false;
        bool countRootReads = false;
        ui32 staleWrites = 0;
        THashSet<ui32> boundChildren;
        THashSet<ui32> emptyNodesAtRoot;
        THashSet<ui32> freshNodes;
        THashSet<TActorId> rootReaders;
        std::vector<std::unique_ptr<IEventHandle>> heldPublications;
        std::vector<std::unique_ptr<IEventHandle>> heldRootReads;
        std::vector<std::unique_ptr<IEventHandle>> heldFreshReplies;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            const ui32 type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeConfigPush::EventType && ev->GetRecipientRewrite() == fixture.Keepers.front()) {
                for (const auto& node : ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetBoundNodes()) {
                    const ui32 id = node.GetNodeId().GetNodeId();
                    if (firstEmptyNode <= id && id <= nodeCount && !node.GetMeta().GetGeneration()) {
                        emptyNodesAtRoot.insert(id);
                    }
                }
            } else if (type == NStorage::TEvNodeConfigReversePush::EventType && nodeId >= firstEmptyNode) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigReversePush>()->Record;
                if (holdPublication && nodeId == firstEmptyNode && record.HasCommittedStorageConfig()) {
                    UNIT_ASSERT_VALUES_EQUAL(record.GetCommittedStorageConfig().GetGeneration(), 1);
                    heldPublications.push_back(std::move(ev));
                    return false;
                }
                if (!record.GetRejected() && nodeId > firstEmptyNode
                    && ev->Sender.NodeId() == nodeId - 1) {
                    boundChildren.insert(nodeId);
                }
            } else if (type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->GetRecipientRewrite() == fixture.Keepers.front()) {
                const auto& query = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.GetQueryConfig();
                if (query.GetRequireFresh() && query.GetRefillNodeId() >= firstEmptyNode) {
                    freshNodes.insert(query.GetRefillNodeId());
                }
            } else if (type == NStorage::TEvNodeWardenReadMetadata::EventType && nodeId == 1) {
                if (countRootReads) {
                    rootReaders.insert(ev->Sender);
                }
                if (holdFresh) {
                    heldRootReads.push_back(std::move(ev));
                    return false;
                }
            } else if (type == NStorage::TEvNodeConfigInvokeOnRootResult::EventType && nodeId >= firstEmptyNode) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (holdFresh && record.HasQueryConfig() && record.GetQueryConfig().GetFresh()) {
                    heldFreshReplies.push_back(std::move(ev));
                    return false;
                }
            } else if (type == NStorage::TEvNodeWardenWriteMetadata::EventType && nodeId >= firstEmptyNode) {
                staleWrites += ev->Get<NStorage::TEvNodeWardenWriteMetadata>()->Record
                    .GetCommittedStorageConfig().GetGeneration() == 1;
            }
            return true;
        };
        Y_DEFER { runtime.FilterFunction = {}; };
        for (ui32 nodeId = firstEmptyNode; nodeId <= nodeCount; ++nodeId) {
            fixture.State.NodeWardenConfigs.erase(nodeId);
            fixture.FormatKeeper(nodeId);
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return !heldPublications.empty() && boundChildren.size() == nodeCount - firstEmptyNode
                && emptyNodesAtRoot.size() == nodeCount - firstEmptyNode + 1;
        }), "empty descendants did not reach the root before the initial publication");
        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig().GetFingerprint(),
            fixture.Config.GetFingerprint());
        UNIT_ASSERT(freshNodes.empty());
        const auto updated = MakeNextConfig(fixture.Config);
        for (ui32 nodeId = 2; nodeId < firstEmptyNode; ++nodeId) {
            fixture.Metadata(nodeId).MutableCommittedStorageConfig()->CopyFrom(updated);
        }
        holdPublication = false;
        holdFresh = true;
        countRootReads = true;
        ReplayDelayedEvents(runtime, heldPublications);
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return freshNodes.size() == nodeCount - firstEmptyNode + 1 && !rootReaders.empty();
        }, TDuration::Seconds(9)), "publication waited for each empty ancestor's refill; fresh requests# "
            << freshNodes.size());
        UNIT_ASSERT_C(rootReaders.size() <= 2, "root metadata readers# " << rootReaders.size());
        UNIT_ASSERT_VALUES_EQUAL(staleWrites, 0);
        for (ui32 nodeId = firstEmptyNode; nodeId <= nodeCount; ++nodeId) {
            UNIT_ASSERT(!fixture.Metadata(nodeId).HasCommittedStorageConfig());
            const auto it = fixture.State.NodeWardenConfigs.find(nodeId);
            UNIT_ASSERT(it == fixture.State.NodeWardenConfigs.end() || !it->second || !it->second->GetGeneration());
        }
        holdFresh = false;
        ReplayDelayedEvents(runtime, heldRootReads);
        ReplayDelayedEvents(runtime, heldFreshReplies);
        CheckDurableConfigConverges(fixture, updated);
        UNIT_ASSERT(SimUntil(runtime, [&] {
            for (ui32 nodeId = firstEmptyNode; nodeId <= nodeCount; ++nodeId) {
                const auto it = fixture.State.NodeWardenConfigs.find(nodeId);
                if (it == fixture.State.NodeWardenConfigs.end() || !it->second
                    || it->second->GetFingerprint() != updated.GetFingerprint()) {
                    return false;
                }
            }
            return true;
        }));
        UNIT_ASSERT_VALUES_EQUAL(staleWrites, 0);
        UNIT_ASSERT_C(rootReaders.size() <= 2, "root metadata readers# " << rootReaders.size());
    }

    Y_UNIT_TEST(RefillBatchesAfterRootError) {
        constexpr ui32 firstEmptyNode = 13;
        constexpr size_t emptyNodeCount = 8;
        TCommitConfirmationFixture fixture(true, 20);
        fixture.UseMirror3dcGroup(20);
        fixture.State.RequiredRootNodeId = 1;
        for (ui32 nodeId = firstEmptyNode; nodeId <= 20; ++nodeId) {
            fixture.State.RequiredParentNodeIds[nodeId] = 2;
        }
        auto& runtime = fixture.Runtime;
        fixture.Start(true);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        QueryKeeper(runtime, fixture.Keepers.front());

        CheckMinimalConfigQuorum(fixture, 2, 12);

        std::array<TActorId, emptyNodeCount> refillActors{};
        std::array<std::vector<ui64>, emptyNodeCount> producerRequestCookies;
        std::array<ui32, emptyNodeCount> rootRequests{};
        std::array<ui32, emptyNodeCount> racedReplies{};
        std::array<ui32, emptyNodeCount> freshReplies{};
        THashSet<TActorId> rootReaders;
        THashSet<TActorId> readersAfterError;
        std::vector<std::unique_ptr<IEventHandle>> heldRootReads;
        bool holdRootReads = true;
        bool countAfterError = false;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            const ui32 type = ev->GetTypeRewrite();
            if (type == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& request = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (request.HasQueryConfig() && request.GetQueryConfig().GetRequireFresh()) {
                    const ui32 origin = request.GetQueryConfig().GetRefillNodeId();
                    if (firstEmptyNode <= origin && origin <= 20) {
                        const size_t index = origin - firstEmptyNode;
                        if (ev->GetRecipientRewrite() == fixture.Keepers[origin - 1]) {
                            UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), origin);
                            if (!refillActors[index]) {
                                refillActors[index] = ev->Sender;
                            } else {
                                UNIT_ASSERT_VALUES_EQUAL(refillActors[index], ev->Sender);
                            }
                            auto& cookies = producerRequestCookies[index];
                            if (!cookies.empty()) {
                                UNIT_ASSERT_C(ev->Cookie > cookies.back(), "non-increasing refill cookie for " << origin);
                            }
                            cookies.push_back(ev->Cookie);
                        } else if (ev->GetRecipientRewrite() == fixture.Keepers.front()) {
                            UNIT_ASSERT(refillActors[index]);
                            UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), 2);
                            ++rootRequests[index];
                        }
                    }
                }
            } else if (type == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                for (size_t index = 0; index < emptyNodeCount; ++index) {
                    if (refillActors[index] && ev->GetRecipientRewrite() == refillActors[index]) {
                        const auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
                        if (result.GetStatus() == TResult::RACE) {
                            UNIT_ASSERT(!producerRequestCookies[index].empty());
                            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, producerRequestCookies[index].front());
                            ++racedReplies[index];
                        } else {
                            UNIT_ASSERT_C(result.GetStatus() == TResult::OK, result.DebugString());
                            UNIT_ASSERT(result.HasScepter());
                            UNIT_ASSERT_VALUES_EQUAL(result.GetScepter().GetNodeId(), 1);
                            UNIT_ASSERT(result.GetQueryConfig().GetFresh());
                            UNIT_ASSERT_VALUES_EQUAL(result.GetQueryConfig().GetConfig().GetFingerprint(),
                                fixture.Config.GetFingerprint());
                            UNIT_ASSERT(!producerRequestCookies[index].empty());
                            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, producerRequestCookies[index].back());
                            ++freshReplies[index];
                        }
                    }
                }
            } else if (nodeId == 1 && type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                if (rootReaders.insert(ev->Sender).second && countAfterError) {
                    readersAfterError.insert(ev->Sender);
                }
                if (holdRootReads) {
                    heldRootReads.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        Y_DEFER { runtime.FilterFunction = {}; };
        for (ui32 nodeId = firstEmptyNode; nodeId <= 20; ++nodeId) {
            fixture.State.NodeWardenConfigs.erase(nodeId);
            fixture.FormatKeeper(nodeId);
        }
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return !heldRootReads.empty() && std::ranges::all_of(rootRequests,
                [](ui32 count) { return count != 0; });
        }, TDuration::Seconds(9)), "not every empty replica issued its first real refill request");
        UNIT_ASSERT_VALUES_EQUAL(rootReaders.size(), 1);
        for (size_t index = 0; index < emptyNodeCount; ++index) {
            UNIT_ASSERT_VALUES_EQUAL(rootRequests[index], 1);
            UNIT_ASSERT_VALUES_EQUAL(producerRequestCookies[index].size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(producerRequestCookies[index].front(), 1);
            UNIT_ASSERT_VALUES_EQUAL(freshReplies[index], 0);
            UNIT_ASSERT(!fixture.Metadata(firstEmptyNode + index).HasCommittedStorageConfig());
        }
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());

        // The quorum is restored; keep the normal error backoff before reacquiring the scepter.
        countAfterError = true;
        const TInstant errorStarted = runtime.GetClock();
        UNIT_ASSERT(runtime.WrapInActorContext(fixture.Keepers.front(), [&](IActor *actor) {
            static_cast<NStorage::TDistributedConfigKeeper*>(actor)->SwitchToError("transient root error during refill");
        }));
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return std::ranges::all_of(rootRequests,
                [](ui32 count) { return count >= 2; });
        }, TDuration::MilliSeconds(1400)), "real refill retries missed the ERROR_TIMEOUT window");
        UNIT_ASSERT(runtime.GetClock() < errorStarted + TDuration::MilliSeconds(1500));
        UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        UNIT_ASSERT(runtime.GetClock() < errorStarted + TDuration::MilliSeconds(1500));
        UNIT_ASSERT(readersAfterError.empty());
        for (size_t index = 0; index < emptyNodeCount; ++index) {
            UNIT_ASSERT_VALUES_EQUAL(rootRequests[index], 2);
            UNIT_ASSERT_VALUES_EQUAL(producerRequestCookies[index].size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(producerRequestCookies[index].back(), 2);
            UNIT_ASSERT_VALUES_EQUAL(racedReplies[index], 1);
            UNIT_ASSERT_VALUES_EQUAL(freshReplies[index], 0);
            UNIT_ASSERT(!fixture.Metadata(firstEmptyNode + index).HasCommittedStorageConfig());
        }

        holdRootReads = false;
        ReplayDelayedEvents(runtime, heldRootReads);

        const auto emptyReplicasConverged = [&](const NKikimrBlobStorage::TStorageConfig& config) {
            for (ui32 nodeId = firstEmptyNode; nodeId <= 20; ++nodeId) {
                const auto it = fixture.State.NodeWardenConfigs.find(nodeId);
                if (fixture.Metadata(nodeId).GetCommittedStorageConfig().GetFingerprint() != config.GetFingerprint()
                    || it == fixture.State.NodeWardenConfigs.end() || !it->second
                    || it->second->GetFingerprint() != config.GetFingerprint()) {
                    return false;
                }
            }
            return true;
        };
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            return emptyReplicasConverged(fixture.Config)
                && std::ranges::all_of(freshReplies, [](ui32 count) { return count == 1; });
        }, TDuration::Seconds(30)), "empty replicas did not durably refill and report to NodeWarden"
            << " root collections after error# " << readersAfterError.size());

        UNIT_ASSERT_VALUES_EQUAL(QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig()
            .GetConfig().GetFingerprint(), fixture.Config.GetFingerprint());
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        UNIT_ASSERT_VALUES_EQUAL(readersAfterError.size(), 2);
        for (size_t index = 0; index < emptyNodeCount; ++index) {
            UNIT_ASSERT_VALUES_EQUAL(rootRequests[index], 2);
            UNIT_ASSERT_VALUES_EQUAL(producerRequestCookies[index].size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(racedReplies[index], 1);
            UNIT_ASSERT_VALUES_EQUAL(freshReplies[index], 1);
        }
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT(emptyReplicasConverged(fixture.Config));
        UNIT_ASSERT_VALUES_EQUAL(readersAfterError.size(), 2);

        countAfterError = false;
        auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        advance->Record.MutableAdvanceGeneration();
        InvokeKeeper(runtime, fixture.Keepers.front(), std::move(advance));
        const auto advanced = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(advanced.GetGeneration(), fixture.Config.GetGeneration() + 1);
        CheckDurableConfigConverges(fixture, advanced);
        UNIT_ASSERT(SimUntil(runtime, [&] { return emptyReplicasConverged(advanced); }));
    }

    Y_UNIT_TEST(RefillBypassesStalledRootRead) {
        TCommitConfirmationFixture fixture(true, 20);
        fixture.UseMirror3dcGroup(20);
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        fixture.Start(true);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        QueryKeeper(runtime, fixture.Keepers[0]);

        CheckMinimalConfigQuorum(fixture, 2, 12);

        bool stallRootReads = true;
        ui32 rootReads = 0;
        ui32 rootFreshRequests = 0;
        THashSet<ui32> freshRequestNodes;
        THashSet<ui32> workingRootNodes;
        std::vector<std::unique_ptr<IEventHandle>> heldRootReads;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (stallRootReads && nodeId == 1
                && ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadata::EventType) {
                ++rootReads;
                heldRootReads.push_back(std::move(ev));
                return false;
            }
            if (ev->GetRecipientRewrite() == fixture.Keepers[0]
                && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (record.HasQueryConfig() && record.GetQueryConfig().GetRequireFresh()) {
                    ++rootFreshRequests;
                    freshRequestNodes.insert(ev->Sender.NodeId());
                }
                if (ev->Sender.NodeId() >= 13 && ev->Sender.NodeId() <= 20 && record.HasQueryWorkingRoot()) {
                    workingRootNodes.insert(ev->Sender.NodeId());
                }
            }
            return true;
        };
        for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
            fixture.FormatKeeper(nodeId);
        }
        for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
            UNIT_ASSERT(!fixture.Metadata(nodeId).HasCommittedStorageConfig());
        }
        const TInstant started = runtime.GetClock();
        UNIT_ASSERT_C(SimUntil(runtime, [&] {
            for (ui32 nodeId = 13; nodeId <= 20; ++nodeId) {
                if (fixture.Metadata(nodeId).GetCommittedStorageConfig().GetFingerprint()
                    != fixture.Config.GetFingerprint()) {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(100)), "empty replicas did not refill with a live physical configuration quorum"
            << " elapsed# " << runtime.GetClock() - started
            << " rootReads# " << rootReads << " freshRequests# " << rootFreshRequests);
        UNIT_ASSERT_C(rootReads && rootReads <= 4, "too many global collections: " << rootReads);
        UNIT_ASSERT(rootFreshRequests >= 8);
        UNIT_ASSERT_VALUES_EQUAL(freshRequestNodes.size(), 8);
        UNIT_ASSERT(!heldRootReads.empty());
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers[0], true).HasScepter());
        CheckDurableConfigConverges(fixture, fixture.Config);

        workingRootNodes.clear();
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(65));
        UNIT_ASSERT_VALUES_EQUAL(FindConvergedRoot(runtime, fixture.Keepers).value_or(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(workingRootNodes.size(), 8);
        UNIT_ASSERT(rootReads <= 4);
        stallRootReads = false;
        ReplayDelayedEvents(runtime, heldRootReads);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers[0], true).HasScepter());
    }

    void CheckLegacyChildScatterBackpressure(bool reconnect) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        TActorId edge;
        bool holdScatter = false;
        bool holdGather = false;
        bool holdNode2Reads = false;
        bool trackBindingChange = false;
        bool newBindingSeen = false;
        ui64 oldBindingCookie = 0;
        ui64 heldScatterCookie = 0;
        TActorId oldSendingSession;
        ui32 heldScatterDeliveries = 0;
        THashSet<ui64> sentCookies;
        THashSet<ui32> disconnectedNodes;
        std::vector<std::unique_ptr<IEventHandle>> pendingScatters;
        std::vector<std::unique_ptr<IEventHandle>> pendingGathers;
        std::vector<std::unique_ptr<IEventHandle>> pendingReads;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (ev->InterconnectSession && nodeId == 1 && ev->Sender.NodeId() == 3
                && type == NStorage::TEvNodeConfigPush::EventType) {
                auto& record = ev->Get<NStorage::TEvNodeConfigPush>()->Record;
                record.ClearSubtreeHeight();
                record.ClearSupportsMetadataOnlyCollect();
                if (trackBindingChange && record.GetInitial() && ev->Cookie != oldBindingCookie) {
                    newBindingSeen = true;
                }
            }
            if (trackBindingChange && type == TEvInterconnect::TEvNodeDisconnected::EventType
                && ev->GetRecipientRewrite() == fixture.Keepers[nodeId - 1]
                && ((nodeId == 1 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 3)
                    || (nodeId == 3 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 1))) {
                disconnectedNodes.insert(nodeId);
            }
            if (edge && nodeId == 1 && ev->Sender == fixture.Keepers.front()
                && ev->Recipient == MakeBlobStorageNodeWardenID(3)
                && type == TEvInterconnect::EvForward
                && ev->Type == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasCollectConfigs()) {
                const ui64 cookie = ev->Get<NStorage::TEvNodeConfigScatter>()->Record.GetCookie();
                sentCookies.insert(cookie);
                if (holdScatter) {
                    oldBindingCookie = ev->Cookie;
                    heldScatterCookie = cookie;
                    oldSendingSession = ev->GetRecipientRewrite();
                    pendingScatters.push_back(std::move(ev));
                    return false;
                }
            }
            if (reconnect && heldScatterCookie && nodeId == 3 && ev->InterconnectSession
                && ev->GetRecipientRewrite() == fixture.Keepers[2]
                && type == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.GetCookie() == heldScatterCookie) {
                ++heldScatterDeliveries;
            }
            if (holdGather && ev->InterconnectSession && nodeId == 1 && ev->Sender.NodeId() == 3
                && ev->GetRecipientRewrite() == fixture.Keepers.front()
                && type == NStorage::TEvNodeConfigGather::EventType) {
                pendingGathers.push_back(std::move(ev));
                return false;
            }
            if (holdNode2Reads && nodeId == 2 && type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                pendingReads.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        edge = runtime.AllocateEdgeActor(1);
        holdScatter = true;
        const auto startQuery = [&] {
            response.reset();
            auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            query->Record.MutableQueryConfig()->SetRequireFresh(true);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, query.release()), 1);
        };
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        const auto finishQuery = [&](TResult::EStatus expected) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(25)));
            UNIT_ASSERT_C(response->GetStatus() == expected, response->DebugString());
        };
        for (ui32 round = 0; round != 3; ++round) {
            startQuery();
            finishQuery(TResult::OK);
            UNIT_ASSERT_VALUES_EQUAL(response->GetQueryConfig().GetConfig().GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sentCookies.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(pendingScatters.size(), 1);
        }
        holdScatter = false;
        if (reconnect) {
            trackBindingChange = true;
            fixture.Disconnect(1, 3);
            UNIT_ASSERT(SimUntil(runtime, [&] { return disconnectedNodes.size() == 2; }));
            UNIT_ASSERT(SimUntil(runtime, [&] { return !runtime.HasImmediateEvents(); }, TDuration::Seconds(1)));
            runtime.ReconnectNodes(1, 3);
            UNIT_ASSERT(SimUntil(runtime, [&] { return newBindingSeen; }));
            UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, fixture.Keepers) == 1; }));
        } else {
            holdGather = true;
            ReplayDelayedEvents(runtime, pendingScatters);
            UNIT_ASSERT(SimUntil(runtime, [&] { return !pendingGathers.empty(); }));
            UNIT_ASSERT_VALUES_EQUAL(sentCookies.size(), 1);
            holdNode2Reads = true;
            startQuery();
            UNIT_ASSERT(SimUntil(runtime, [&] { return !pendingReads.empty(); }, TDuration::Seconds(5)));
            holdGather = false;
            ReplayDelayedEvents(runtime, pendingGathers);
            finishQuery(TResult::NO_QUORUM);
            UNIT_ASSERT_VALUES_EQUAL(sentCookies.size(), 1);
            holdNode2Reads = false;
            ReplayDelayedEvents(runtime, pendingReads);
        }
        startQuery();
        finishQuery(TResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(response->GetQueryConfig().GetConfig().GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sentCookies.size(), 2);
        if (reconnect) {
            UNIT_ASSERT(oldSendingSession);
            UNIT_ASSERT_VALUES_EQUAL(pendingScatters.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(pendingScatters.front()->GetRecipientRewrite(), oldSendingSession);
            ReplayDelayedEvents(runtime, pendingScatters);
        }
        CheckDurableConfigConverges(fixture, fixture.Config);
        if (reconnect) {
            UNIT_ASSERT_VALUES_EQUAL(heldScatterDeliveries, 0);
        }
    }

    Y_UNIT_TEST(LegacyScatterWaitsForLateGather) {
        CheckLegacyChildScatterBackpressure(false);
    }

    Y_UNIT_TEST(LegacyScatterResumesAfterRebind) {
        CheckLegacyChildScatterBackpressure(true);
    }

    Y_UNIT_TEST(NodeWardenForwardFencesSession) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        TActorId edge;
        TActorId nodeWarden3;
        TActorId oldReceiverSession;
        TActorId newReceiverSession;
        bool pauseNodeWarden = false;
        bool reconnecting = false;
        bool oldScatterForwarded = false;
        bool newBindingPushObserved = false;
        ui64 oldScatterCookie = 0;
        ui32 staleGathers = 0;
        THashSet<ui32> disconnectedKeepers;
        std::vector<std::unique_ptr<IEventHandle>> nodeWardenBacklog;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (reconnecting && type == TEvInterconnect::TEvNodeDisconnected::EventType
                && ev->GetRecipientRewrite() == fixture.Keepers[nodeId - 1]
                && ((nodeId == 1 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 3)
                    || (nodeId == 3 && ev->Get<TEvInterconnect::TEvNodeDisconnected>()->NodeId == 1))) {
                disconnectedKeepers.insert(nodeId);
            }
            if (reconnecting && nodeId == 3 && ev->GetRecipientRewrite() == fixture.Keepers[2]
                && type == TEvInterconnect::TEvNodeConnected::EventType
                && ev->Get<TEvInterconnect::TEvNodeConnected>()->NodeId == 1
                && ev->Sender != oldReceiverSession) {
                newReceiverSession = ev->Sender;
            }
            if (newReceiverSession && nodeId == 3 && ev->Sender == fixture.Keepers[2]
                && ev->Recipient == MakeBlobStorageNodeWardenID(1)
                && ev->GetRecipientRewrite() == newReceiverSession
                && type == TEvInterconnect::EvForward
                && ev->Type == NStorage::TEvNodeConfigPush::EventType
                && ev->Get<NStorage::TEvNodeConfigPush>()->Record.GetInitial()) {
                newBindingPushObserved = true;
            }
            if (pauseNodeWarden && nodeId == 3 && ev->GetRecipientRewrite() == nodeWarden3) {
                if (!oldReceiverSession && ev->InterconnectSession && ev->Sender.NodeId() == 1
                    && type == NStorage::TEvNodeConfigScatter::EventType
                    && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasCollectConfigs()) {
                    oldReceiverSession = ev->InterconnectSession;
                    oldScatterCookie = ev->Get<NStorage::TEvNodeConfigScatter>()->Record.GetCookie();
                }
                nodeWardenBacklog.push_back(std::move(ev));
                return false;
            }
            if (oldScatterCookie && nodeId == 3 && ev->GetRecipientRewrite() == fixture.Keepers[2]
                && type == NStorage::TEvNodeConfigScatter::EventType
                && ev->InterconnectSession == oldReceiverSession
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.GetCookie() == oldScatterCookie) {
                oldScatterForwarded = true;
            }
            if (oldScatterCookie && nodeId == 1 && ev->GetRecipientRewrite() == fixture.Keepers.front()
                && ev->InterconnectSession && ev->Sender.NodeId() == 3
                && type == NStorage::TEvNodeConfigGather::EventType
                && ev->Get<NStorage::TEvNodeConfigGather>()->Record.GetCookie() == oldScatterCookie) {
                ++staleGathers;
            }
            return true;
        };
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Root().NodeId(), 1);
        nodeWarden3 = runtime.GetNode(3)->ActorSystem->LookupLocalService(MakeBlobStorageNodeWardenID(3));
        UNIT_ASSERT(nodeWarden3);
        edge = runtime.AllocateEdgeActor(1);
        const auto startQuery = [&] {
            response.reset();
            auto query = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            query->Record.MutableQueryConfig()->SetRequireFresh(true);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, query.release()), 1);
        };
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        pauseNodeWarden = true;
        startQuery();
        UNIT_ASSERT(SimUntil(runtime, [&] { return bool(oldReceiverSession); }, TDuration::Seconds(5)));
        UNIT_ASSERT(!nodeWardenBacklog.empty());
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(25)));
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        reconnecting = true;
        fixture.Disconnect(1, 3);
        UNIT_ASSERT(SimUntil(runtime, [&] { return disconnectedKeepers.size() == 2; }));
        UNIT_ASSERT(SimUntil(runtime, [&] { return !runtime.HasImmediateEvents(); }, TDuration::Seconds(1)));
        runtime.ReconnectNodes(1, 3);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return newBindingPushObserved; }, TDuration::Seconds(90)),
            "the keeper did not confirm a new session while NodeWarden remained paused");
        UNIT_ASSERT(newReceiverSession != oldReceiverSession);
        UNIT_ASSERT(!oldScatterForwarded);
        pauseNodeWarden = false;
        ReplayDelayedEvents(runtime, nodeWardenBacklog);
        UNIT_ASSERT_C(SimUntil(runtime, [&] { return oldScatterForwarded; }),
            "the old envelope was not forwarded from the resumed NodeWarden queue");
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, fixture.Keepers) == 1; },
            TDuration::Seconds(90)));
        startQuery();
        UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(25)));
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response->GetQueryConfig().GetConfig().GetGeneration(), 1);
        CheckDurableConfigConverges(fixture, fixture.Config);
        UNIT_ASSERT_VALUES_EQUAL(staleGathers, 0);
    }

    enum class ECompactConfirmationCase {
        Metadata,
        LegacyIntermediate,
        UnexpectedMetadata,
        CorruptBody,
        HigherProposal,
    };

    void CheckCompactCommitConfirmation(ECompactConfirmationCase scenario) {
        TCommitConfirmationFixture fixture;
        fixture.State.RequiredRootNodeId = 1;
        const bool legacy = scenario == ECompactConfirmationCase::LegacyIntermediate
            || scenario == ECompactConfirmationCase::UnexpectedMetadata
            || scenario == ECompactConfirmationCase::CorruptBody;
        if (legacy) {
            fixture.State.RequiredParentNodeIds = {{1, 0}, {2, 1}, {3, 2}};
        }
        auto& runtime = fixture.Runtime;
        TActorId edge;
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        bool checking = false;
        ui32 compactRequests = 0;
        ui32 fullRequests = 0;
        ui32 compactReplies = 0;
        ui32 legacyReplies = 0;
        ui32 invalidModes = 0;
        ui32 corruptedBodies = 0;
        const auto checkBodyOmitted = [&](const auto& config) {
            UNIT_ASSERT(!config.HasBlobStorageConfig());
            UNIT_ASSERT(!config.HasConfigComposite());
            UNIT_ASSERT(!config.HasPrevConfig());
            UNIT_ASSERT_VALUES_EQUAL(config.AllNodesSize(), 0);
            UNIT_ASSERT(config.ByteSizeLong() < fixture.Config.ByteSizeLong() / 4);
        };
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (ReplyToReplaceController(runtime, nodeId, *ev, edge)) {
                return false;
            }
            if (edge && CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (legacy && nodeId == 1 && ev->Sender.NodeId() == 2
                && type == NStorage::TEvNodeConfigPush::EventType) {
                ev->Get<NStorage::TEvNodeConfigPush>()->Record.ClearSupportsMetadataOnlyCollect();
            }
            if (!checking) {
                return true;
            }
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigScatter::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (record.HasCollectConfigs()) {
                    if (record.GetCollectConfigs().GetMetadataOnly()) {
                        ++compactRequests;
                    } else {
                        ++fullRequests;
                    }
                    if (legacy) {
                        UNIT_ASSERT(!record.GetCollectConfigs().GetMetadataOnly());
                    }
                }
            }
            if (ev->InterconnectSession && type == NStorage::TEvNodeConfigGather::EventType) {
                auto& record = ev->Get<NStorage::TEvNodeConfigGather>()->Record;
                if (record.HasCollectConfigs()) {
                    auto& collect = *record.MutableCollectConfigs();
                    if (collect.GetMetadataOnly() && !legacy) {
                        ++compactReplies;
                        for (const auto& node : collect.GetNodes()) {
                            checkBodyOmitted(node.GetBaseConfig());
                        }
                        for (const auto *configs : {&collect.GetCommittedConfigs(), &collect.GetProposedConfigs()}) {
                            for (const auto& item : *configs) {
                                checkBodyOmitted(item.GetConfig());
                            }
                        }
                    } else if (legacy && !collect.GetMetadataOnly()) {
                        ++legacyReplies;
                        UNIT_ASSERT(collect.CommittedConfigsSize());
                        UNIT_ASSERT(collect.GetCommittedConfigs(0).GetConfig().HasBlobStorageConfig());
                        if (scenario == ECompactConfirmationCase::UnexpectedMetadata && nodeId == 1) {
                            collect.SetMetadataOnly(true);
                            ++invalidModes;
                        } else if (scenario == ECompactConfirmationCase::CorruptBody && nodeId == 1) {
                            for (auto& item : *collect.MutableCommittedConfigs()) {
                                item.MutableConfig()->SetClusterUUID("damaged-body");
                                ++corruptedBodies;
                            }
                        }
                    }
                }
            }
            return true;
        };
        if (legacy) {
            fixture.StartWithBoundChild(1, 2);
        } else {
            fixture.Start();
        }
        edge = runtime.AllocateEdgeActor(1);
        const auto replace = [&](ui64 version) {
            response.reset();
            auto request = fixture.MakeReplaceRequest(version);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
            UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(35)));
        };
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        replace(0);
        UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
        const auto committed = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(committed.GetGeneration(), 2);
        CheckDurableConfigConverges(fixture, committed);
        auto fresh = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        fresh->Record.MutableQueryConfig()->SetRequireFresh(true);
        InvokeKeeper(runtime, fixture.Keepers.front(), std::move(fresh));
        if (scenario == ECompactConfirmationCase::HigherProposal) {
            auto proposal = committed;
            proposal.SetGeneration(3);
            proposal.MutableSelfManagementConfig()->SetAutomaticBootstrap(true);
            NStorage::TDistributedConfigKeeper::UpdateFingerprint(&proposal);
            fixture.Metadata(3).MutableProposedStorageConfig()->CopyFrom(proposal);
        }
        checking = true;
        replace(0);
        if (scenario == ECompactConfirmationCase::CorruptBody
            || scenario == ECompactConfirmationCase::UnexpectedMetadata) {
            UNIT_ASSERT_C(response->GetStatus() == TResult::NO_QUORUM, response->DebugString());
            if (scenario == ECompactConfirmationCase::UnexpectedMetadata) {
                UNIT_ASSERT(invalidModes);
            } else {
                UNIT_ASSERT(corruptedBodies);
            }
        } else {
            UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
            if (legacy) {
                UNIT_ASSERT(fullRequests && legacyReplies);
                UNIT_ASSERT_VALUES_EQUAL(compactRequests, 0);
            } else {
                UNIT_ASSERT(compactRequests && compactReplies);
                if (scenario == ECompactConfirmationCase::HigherProposal) {
                    UNIT_ASSERT(fullRequests);
                    checking = false;
                    replace(1);
                    UNIT_ASSERT_C(response->GetStatus() == TResult::OK, response->DebugString());
                    const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
                    UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 4);
                    CheckDurableConfigConverges(fixture, winner);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(fullRequests, 0);
                }
            }
        }
    }

    Y_UNIT_TEST(CommitConfirmationUsesMetadata) {
        CheckCompactCommitConfirmation(ECompactConfirmationCase::Metadata);
    }

    Y_UNIT_TEST(CommitConfirmationLegacyFallback) {
        CheckCompactCommitConfirmation(ECompactConfirmationCase::LegacyIntermediate);
    }

    Y_UNIT_TEST(CommitConfirmationRejectsWrongMode) {
        CheckCompactCommitConfirmation(ECompactConfirmationCase::UnexpectedMetadata);
    }

    Y_UNIT_TEST(CommitConfirmationRejectsCorruptBody) {
        CheckCompactCommitConfirmation(ECompactConfirmationCase::CorruptBody);
    }

    Y_UNIT_TEST(CommitConfirmationLoadsHigherProposal) {
        CheckCompactCommitConfirmation(ECompactConfirmationCase::HigherProposal);
    }

    Y_UNIT_TEST(ScatterRetriesLostGather) {
        TCommitConfirmationFixture fixture(true);
        fixture.UseMirror3dcGroup();
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        TActorId edge = runtime.AllocateEdgeActor(1);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult> response;
        bool dropFirstGather = true;
        bool holdNode2Reads = false;
        THashSet<ui64> childScatterCookies;
        std::optional<ui64> droppedGatherCookie;
        std::vector<std::unique_ptr<IEventHandle>> heldReads;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            if (CaptureInvokeResult(*ev, edge, response)) {
                return false;
            }
            const ui32 type = ev->GetTypeRewrite();
            if (nodeId == 3 && ev->InterconnectSession && ev->Sender.NodeId() == 1
                && ev->GetRecipientRewrite() == fixture.Keepers[2]
                && type == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasCollectConfigs()) {
                const ui64 cookie = ev->Get<NStorage::TEvNodeConfigScatter>()->Record.GetCookie();
                UNIT_ASSERT_C(childScatterCookies.insert(cookie).second, "duplicate child scatter: " << cookie);
            }
            if (nodeId == 1 && ev->InterconnectSession && ev->Sender.NodeId() == 3
                && ev->GetRecipientRewrite() == fixture.Keepers.front()
                && type == NStorage::TEvNodeConfigGather::EventType
                && ev->Get<NStorage::TEvNodeConfigGather>()->Record.HasCollectConfigs() && dropFirstGather) {
                droppedGatherCookie = ev->Get<NStorage::TEvNodeConfigGather>()->Record.GetCookie();
                UNIT_ASSERT(childScatterCookies.contains(*droppedGatherCookie));
                dropFirstGather = false;
                return false;
            }
            if (holdNode2Reads && nodeId == 2 && type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                heldReads.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        const auto query = [&] {
            response.reset();
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableQueryConfig()->SetRequireFresh(true);
            runtime.Send(new IEventHandle(fixture.Keepers.front(), edge, request.release()), 1);
            UNIT_ASSERT(SimUntil(runtime, [&] { return response.has_value(); }, TDuration::Seconds(25)));
            UNIT_ASSERT_C(response->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                response->DebugString());
        };
        query();
        UNIT_ASSERT(!dropFirstGather);
        UNIT_ASSERT_VALUES_EQUAL(childScatterCookies.size(), 1);
        UNIT_ASSERT(droppedGatherCookie);
        holdNode2Reads = true;
        query();
        UNIT_ASSERT_VALUES_EQUAL(childScatterCookies.size(), 2);
        UNIT_ASSERT(!heldReads.empty());
        holdNode2Reads = false;
        ReplayDelayedEvents(runtime, heldReads);
        CheckDurableConfigConverges(fixture, fixture.Config);
    }

    enum class EFreshQueryFailure {
        ActiveCollect,
        ErrorBackoffThenCollect,
        NoQuorumBeforeRequests,
    };

    void CheckFreshQueryAbort(EFreshQueryFailure failure) {
        const bool noQuorum = failure == EFreshQueryFailure::NoQuorumBeforeRequests;
        const bool errorWindow = failure == EFreshQueryFailure::ErrorBackoffThenCollect;
        const ui32 nodeCount = noQuorum ? 3 : 6;
        TCommitConfirmationFixture fixture(true, nodeCount);
        fixture.UseMirror3dcGroup(nodeCount);
        fixture.State.RequiredRootNodeId = 1;
        auto& runtime = fixture.Runtime;
        fixture.Start();
        CheckDurableConfigConverges(fixture, fixture.Config);
        QueryKeeper(runtime, fixture.Keepers.front());
        TQueryBatch batch(fixture, {
            errorWindow ? 2u : 1u, noQuorum || errorWindow ? 1u : 2u, errorWindow ? 3u : 1u});
        bool partitioned = noQuorum;
        bool holdReads = !noQuorum;
        ui32 rootRequests = 0;
        ui32 rootReads = 0;
        ui32 outgoingCollects = 0;
        THashSet<TActorId> failedGroupActors;
        std::vector<std::unique_ptr<IEventHandle>> pendingReads;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            SetMetadataDiskGuid(nodeId, *ev);
            const ui32 type = ev->GetTypeRewrite();
            if (ev->GetRecipientRewrite() == fixture.Keepers.front()
                && type == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.GetQueryConfig().GetRequireFresh()) {
                ++rootRequests;
            }
            if (batch.Capture(*ev)) {
                if (partitioned) {
                    failedGroupActors.insert(ev->Sender);
                }
                return false;
            }
            if (partitioned && ev->Sender.NodeId() == 1 && type == NStorage::TEvNodeConfigScatter::EventType
                && ev->Get<NStorage::TEvNodeConfigScatter>()->Record.HasCollectConfigs()) {
                ++outgoingCollects;
            }
            if (nodeId == 1 && type == NStorage::TEvNodeWardenReadMetadata::EventType) {
                rootReads += partitioned;
                if (holdReads) {
                    pendingReads.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        Y_DEFER { runtime.FilterFunction = {}; };
        const auto disconnect = [&] {
            fixture.Disconnect(1, 2);
            fixture.Disconnect(1, 3);
        };
        if (noQuorum) {
            disconnect();
            UNIT_ASSERT(SimUntil(runtime, [&] {
                bool hasQuorum = true;
                UNIT_ASSERT(runtime.WrapInActorContext(fixture.Keepers.front(), [&](IActor *actor) {
                    hasQuorum = static_cast<NStorage::TDistributedConfigKeeper*>(actor)->PartOfNodeQuorum();
                }));
                return !hasQuorum;
            }, TDuration::MilliSeconds(200)));
        }
        if (errorWindow) {
            UNIT_ASSERT(runtime.WrapInActorContext(fixture.Keepers.front(), [&](IActor *actor) {
                static_cast<NStorage::TDistributedConfigKeeper*>(actor)->SwitchToError("transient quorum loss");
            }));
        }
        for (size_t i = 0; i < batch.Requests.size(); ++i) {
            batch.Send(i);
            if (!noQuorum) {
                UNIT_ASSERT(SimUntil(runtime, [&] { return rootRequests == i + 1; }, TDuration::MilliSeconds(200)));
                if (!i && !errorWindow) {
                    UNIT_ASSERT(SimUntil(runtime, [&] { return !pendingReads.empty(); }));
                }
            }
        }
        if (noQuorum) {
            UNIT_ASSERT(SimUntil(runtime, [&] { return rootRequests == 3; }, TDuration::MilliSeconds(200)));
            UNIT_ASSERT(std::ranges::none_of(batch.Requests, [](const auto& request) { return request.Result.has_value(); }));
        } else if (errorWindow) {
            UNIT_ASSERT(pendingReads.empty());
            UNIT_ASSERT(!QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
            UNIT_ASSERT(SimUntil(runtime, [&] { return !pendingReads.empty(); }, TDuration::Seconds(5)));
        }
        if (!noQuorum) {
            UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
            partitioned = true;
            disconnect();
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return batch.AllReplied(); },
            noQuorum ? TDuration::Seconds(5) : TDuration::Seconds(30)));
        for (const auto& request : batch.Requests) {
            UNIT_ASSERT_C(request.Result->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE,
                request.Result->DebugString());
            UNIT_ASSERT_VALUES_EQUAL(request.Count, 1);
        }
        if (noQuorum) {
            UNIT_ASSERT_VALUES_EQUAL(failedGroupActors.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rootReads, 0);
            UNIT_ASSERT_VALUES_EQUAL(outgoingCollects, 0);
        }
        holdReads = false;
        ReplayDelayedEvents(runtime, pendingReads);
        SimUntil(runtime, [] { return false; }, TDuration::Seconds(1));
        for (const auto& request : batch.Requests) {
            UNIT_ASSERT_VALUES_EQUAL(request.Count, 1);
        }
        partitioned = false;
        runtime.ReconnectNodes(1, 2);
        runtime.ReconnectNodes(1, 3);
        UNIT_ASSERT(SimUntil(runtime, [&] { return FindConvergedRoot(runtime, fixture.Keepers) == 1; }));
        UNIT_ASSERT(QueryKeeper(runtime, fixture.Keepers.front(), true).HasScepter());
        for (size_t i = 0; i < batch.Requests.size(); ++i) {
            batch.Requests[i].Result.reset();
            batch.Send(i);
        }
        UNIT_ASSERT(SimUntil(runtime, [&] { return batch.AllReplied(); }));
        for (const auto& request : batch.Requests) {
            UNIT_ASSERT_C(request.Result->GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                request.Result->DebugString());
            UNIT_ASSERT(request.Result->GetQueryConfig().GetFresh());
            UNIT_ASSERT_VALUES_EQUAL(request.Result->GetQueryConfig().GetConfig().GetFingerprint(), fixture.Config.GetFingerprint());
            UNIT_ASSERT_VALUES_EQUAL(request.Count, 2);
        }
        auto advance = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        advance->Record.MutableAdvanceGeneration();
        InvokeKeeper(runtime, fixture.Keepers.front(), std::move(advance));
        const auto winner = QueryKeeper(runtime, fixture.Keepers.front()).GetQueryConfig().GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(winner.GetGeneration(), 2);
        CheckDurableConfigConverges(fixture, winner);
    }

    Y_UNIT_TEST(FreshQueryAbortRecovers) {
        CheckFreshQueryAbort(EFreshQueryFailure::ActiveCollect);
    }

    Y_UNIT_TEST(PendingFreshQueryAbortRecovers) {
        CheckFreshQueryAbort(EFreshQueryFailure::ErrorBackoffThenCollect);
    }

    Y_UNIT_TEST(PendingQueriesRecoverAfterQuorumLoss) {
        CheckFreshQueryAbort(EFreshQueryFailure::NoQuorumBeforeRequests);
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
        command.SetIgnoreGroupLayoutChecks(true);
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
        UNIT_ASSERT(params.LayoutPolicy.AllowsRelaxedPlacement());
        UNIT_ASSERT(params.LayoutPolicy.Accepts(false));
        UNIT_ASSERT(params.AllowUnusableDisks);
        UNIT_ASSERT(!params.SettleOnlyOnOperationalDisks);
        UNIT_ASSERT(params.IsSelfHealReasonDecommit);
        UNIT_ASSERT(!params.PreferLessOccupiedRack);
        UNIT_ASSERT(params.WithAttentionToReplication);
        UNIT_ASSERT(!params.UseSelfHealLocalPolicy);
        UNIT_ASSERT(params.TryToRelocateBrokenDisksLocallyFirst);
        UNIT_ASSERT(params.ApplySelfHealNodeAllowList);
        UNIT_ASSERT_VALUES_EQUAL(params.Reassignments, &reassignments);

        command.SetFromSelfHeal(false);
        command.SetIgnoreGroupLayoutChecks(false);
        params = NStorage::TDistributedConfigKeeper::BuildStaticGroupReassignParams(&config, &baseConfig, command,
                                                                                    group, serviceSet, &reassignments);
        UNIT_ASSERT(!params.LayoutPolicy.AllowsRelaxedPlacement());
        UNIT_ASSERT(!params.LayoutPolicy.Accepts(false));

        command.SetFromSelfHeal(true);
        params = NStorage::TDistributedConfigKeeper::BuildStaticGroupReassignParams(&config, &baseConfig, command,
                                                                                    group, serviceSet, &reassignments);
        UNIT_ASSERT(!params.LayoutPolicy.AllowsRelaxedPlacement());
        UNIT_ASSERT(params.LayoutPolicy.Accepts(false));
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

    void CheckStateStorageUpdate(bool explicitConfig, bool writeOnly) {
        const std::array<const char*, 3> names = {"StateStorage", "StateStorageBoard", "SchemeBoard"};
        for (ui32 ringGroups : {1, 2}) {
            for (ui32 entity = 0; entity < names.size(); ++entity) {
                TTestSetup s;
                s.EnableSelfManagement();
                s.SyncAppConfigBlobStorage();
                s.AddNode(1, "host-1");
                s.AddNode(2, "host-2");
                auto *current = entity == 0 ? s.StorageConfig.MutableStateStorageConfig()
                    : entity == 1 ? s.StorageConfig.MutableStateStorageBoardConfig()
                    : s.StorageConfig.MutableSchemeBoardConfig();
                current->SetSSId(1);
                for (ui32 i = 0; i < ringGroups; ++i) {
                    auto *ring = current->AddRingGroups();
                    ring->SetNToSelect(1);
                    ring->AddNode(i + 1);
                }
                auto proposed = *current;
                proposed.SetStateStorageVersion(1);
                for (auto& ring : *proposed.MutableRingGroups()) {
                    ring.SetWriteOnly(writeOnly);
                }
                auto *domains = s.AppConfig.MutableDomainsConfig();
                domains->AddDomain()->AddSSId(1);
                if (explicitConfig) {
                    auto *target = entity == 0 ? domains->MutableExplicitStateStorageConfig()
                        : entity == 1 ? domains->MutableExplicitStateStorageBoardConfig()
                        : domains->MutableExplicitSchemeBoardConfig();
                    target->CopyFrom(proposed);
                } else {
                    domains->AddStateStorage()->CopyFrom(proposed);
                }
                TString error;
                const bool accepted = s.Derive(&error);
                if (writeOnly) {
                    UNIT_ASSERT_C(!accepted, names[entity] << " accepted all-WriteOnly ring groups");
                    UNIT_ASSERT_STRING_CONTAINS(error, names[entity]);
                    UNIT_ASSERT_VALUES_EQUAL(current->GetStateStorageVersion(), 0);
                    UNIT_ASSERT(!current->GetRingGroups(0).GetWriteOnly());
                } else {
                    UNIT_ASSERT_C(accepted, error);
                    UNIT_ASSERT_VALUES_EQUAL(current->SerializeAsString(), proposed.SerializeAsString());
                }
            }
        }
    }

    Y_UNIT_TEST(LegacyStateStorageRejectsAllWriteOnlyGroups) {
        CheckStateStorageUpdate(false, true);
    }

    Y_UNIT_TEST(LegacyStateStorageAcceptsValidUpdate) {
        CheckStateStorageUpdate(false, false);
    }

    Y_UNIT_TEST(ExplicitStateStorageRejectsAllWriteOnlyGroups) {
        CheckStateStorageUpdate(true, true);
    }

    Y_UNIT_TEST(ExplicitStateStorageAcceptsValidUpdate) {
        CheckStateStorageUpdate(true, false);
    }
}

Y_UNIT_TEST_SUITE(TDeriveStorageConfigBridgeTest) {

    // every node must belong to a pile listed in BridgeConfig, otherwise no node can build its bridge info
    Y_UNIT_TEST(NodePileNames) {
        auto derive = [](const std::vector<std::optional<TString>>& nodePiles, const TVector<TString>& piles,
                TString *error) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableBlobStorageConfig();
            for (const TString& name : piles) {
                appConfig.MutableBridgeConfig()->AddPiles()->SetName(name);
            }
            for (size_t i = 0; i < nodePiles.size(); ++i) {
                auto *node = appConfig.MutableNameserviceConfig()->AddNode();
                node->SetNodeId(i + 1);
                node->SetInterconnectHost("host-" + ToString(i + 1));
                node->SetPort(19000);
                if (nodePiles[i]) {
                    node->MutableLocation()->SetBridgePileName(*nodePiles[i]);
                }
            }
            NKikimrBlobStorage::TStorageConfig config;
            return NStorage::DeriveStorageConfig(appConfig, &config, error);
        };

        TString error;
        UNIT_ASSERT_C(derive({"a", "b"}, {"a", "b"}, &error), error);
        UNIT_ASSERT_C(derive({"", "b"}, {"", "b"}, &error), error); // empty name is a valid one
        UNIT_ASSERT_C(derive({std::nullopt}, {"a"}, &error), error); // the only pile is the implicit one
        UNIT_ASSERT(!derive({"a", "c"}, {"a", "b"}, &error));
        UNIT_ASSERT_STRING_CONTAINS(error, "unknown pile name 'c' for node 2");
        UNIT_ASSERT(!derive({"a", ""}, {"a", "b"}, &error));
        UNIT_ASSERT_STRING_CONTAINS(error, "unknown pile name '' for node 2");
        UNIT_ASSERT(!derive({std::nullopt, "a"}, {"a", "b"}, &error));
        UNIT_ASSERT_STRING_CONTAINS(error, "mandatory pile name is missing for node 1");
    }

    // parts of static groups get new generations when the pile becomes unsynced; donors and VDisks being destroyed are
    // separate entries of older generations, which must stay as they are (otherwise they look as active ones)
    Y_UNIT_TEST(ResetStaticGroupPilePartsKeepsDonors) {
        NKikimrBlobStorage::TStorageConfig config;
        auto *ss = config.MutableBlobStorageConfig()->MutableServiceSet();
        auto addGroup = [&](ui32 groupId) {
            auto *group = ss->AddGroups();
            group->SetGroupID(groupId);
            group->SetGroupGeneration(2);
            return group;
        };
        auto *proxyGroup = addGroup(0); // inline pile of itself in pile 0, part in pile 1 is group 1
        addGroup(1);
        for (ui32 pileGroupId : {0, 1}) {
            auto *pile = proxyGroup->MutableBridgeGroupState()->AddPile();
            pile->SetGroupId(pileGroupId);
            pile->SetGroupGeneration(2);
            pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
        }
        auto addVDisk = [&](ui32 groupId, ui32 generation, ui32 slotId) {
            auto *vdisk = ss->AddVDisks();
            VDiskIDFromVDiskID(TVDiskID(TGroupId::FromValue(groupId), generation, 0, 0, 0), vdisk->MutableVDiskID());
            vdisk->MutableVDiskLocation()->SetNodeID(1);
            vdisk->MutableVDiskLocation()->SetPDiskID(1);
            vdisk->MutableVDiskLocation()->SetVDiskSlotID(slotId);
            return vdisk;
        };
        addVDisk(0, 2, 1); // active VDisks
        addVDisk(1, 2, 2);
        addVDisk(0, 1, 3)->MutableDonorMode()->SetErasureSpecies(0); // donor left after reassignment
        addVDisk(1, 1, 4)->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);

        NStorage::ResetStaticGroupPileParts(&config, {TBridgePileId::FromPileIndex(1)}, 7);

        for (const auto& group : ss->GetGroups()) {
            UNIT_ASSERT_VALUES_EQUAL_C(group.GetGroupGeneration(), 3, group.ShortDebugString());
        }
        const auto& state = ss->GetGroups(0).GetBridgeGroupState();
        UNIT_ASSERT_EQUAL(state.GetPile(0).GetStage(), NKikimrBridge::TGroupState::SYNCED);
        UNIT_ASSERT_EQUAL(state.GetPile(1).GetStage(), NKikimrBridge::TGroupState::BLOCKS);
        UNIT_ASSERT_VALUES_EQUAL(state.GetPile(1).GetBecameUnsyncedGeneration(), 7);
        for (const auto& pile : state.GetPile()) {
            UNIT_ASSERT_VALUES_EQUAL(pile.GetGroupGeneration(), 3);
        }
        const std::vector<ui32> expected{3, 3, 1, 1};
        for (size_t i = 0; i < expected.size(); ++i) {
            const auto& vdisk = ss->GetVDisks(i);
            UNIT_ASSERT_VALUES_EQUAL_C(VDiskIDFromVDiskID(vdisk.GetVDiskID()).GroupGeneration, expected[i],
                vdisk.ShortDebugString());
        }
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

        void SetNodeRack(ui32 nodeId, const TString& rack) {
            for (auto& node : *Config.MutableAllNodes()) {
                if (node.GetNodeId() == nodeId) {
                    node.MutableLocation()->SetRack(rack);
                    return;
                }
            }
            UNIT_FAIL("Node not found");
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
        bool IgnoreGroupLayoutChecks = false;
        bool RequireCorrectLayout = true;
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
            .LayoutPolicy = NBsController::TGroupLayoutPolicy::FromFlags(options.IgnoreGroupLayoutChecks,
                                                                         options.RequireCorrectLayout),
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

    Y_UNIT_TEST(IncorrectTargetLayoutRequiresExplicitOverride) {
        TSetup s = MakeBlock42Setup();
        s.SetNodeRack(9, "rack-2");
        TReallocateOptions options{
            .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
            .NumFailDomains = 8,
            .TargetPDiskId = NBsController::TPDiskId(9, 1),
        };
        UNIT_ASSERT_EXCEPTION(Reallocate(s, NodeIds({}), false, options),
                              NStorage::TDistributedConfigKeeper::TExConfigError);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes().front(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupGeneration(), 1u);

        options.IgnoreGroupLayoutChecks = true;
        Reallocate(s, NodeIds({}), false, options);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes().front(), 9u);
    }

    Y_UNIT_TEST(AlreadyIncorrectLayoutRequiresRepairOrOverride) {
        TSetup s = MakeBlock42Setup();
        s.SetNodeRack(1, "rack-2");
        TReallocateOptions options{
            .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
            .NumFailDomains = 8,
            .TargetPDiskId = NBsController::TPDiskId(9, 1),
            .VDiskId = TVDiskIdShort(0, 2, 0),
        };
        UNIT_ASSERT_EXCEPTION_CONTAINS(Reallocate(s, NodeIds({}), false, options),
                                       NStorage::TDistributedConfigKeeper::TExConfigError,
                                       "Group layout is incorrect");
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes()[2], 3u);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupGeneration(), 1u);

        auto withOverride = s;
        options.IgnoreGroupLayoutChecks = true;
        Reallocate(withOverride, NodeIds({}), false, options);
        UNIT_ASSERT_VALUES_EQUAL(withOverride.GetGroupDomainNodes()[2], 9u);

        options.IgnoreGroupLayoutChecks = false;
        options.RequireCorrectLayout = false;
        auto fromSelfHeal = s;
        Reallocate(fromSelfHeal, NodeIds({}), false, options);
        UNIT_ASSERT_VALUES_EQUAL(fromSelfHeal.GetGroupDomainNodes()[2], 9u);

        options.RequireCorrectLayout = true;
        options.VDiskId = TVDiskIdShort(0, 0, 0);
        Reallocate(s, NodeIds({}), false, options);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes().front(), 9u);
    }

    Y_UNIT_TEST(AutomaticPlacementCanIgnoreLayout) {
        for (bool alreadyIncorrect : {false, true}) {
            TSetup s = MakeBlock42Setup();
            s.SetNodeRack(9, "rack-2");
            s.SetPDiskDriveStatus(10, NKikimrBlobStorage::BROKEN);
            if (alreadyIncorrect) {
                s.SetNodeRack(3, "rack-2");
            }
            TReallocateOptions options{
                .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
                .NumFailDomains = 8,
            };
            UNIT_ASSERT_EXCEPTION(Reallocate(s, NodeIds({}), false, options),
                                  NStorage::TDistributedConfigKeeper::TExConfigError);
            UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes().front(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(s.GetGroupGeneration(), 1u);

            options.RequireCorrectLayout = false;
            UNIT_ASSERT_EXCEPTION(Reallocate(s, NodeIds({}), false, options),
                                  NStorage::TDistributedConfigKeeper::TExConfigError);
            options.RequireCorrectLayout = true;
            options.IgnoreGroupLayoutChecks = true;
            Reallocate(s, NodeIds({}), false, options);
            UNIT_ASSERT_VALUES_EQUAL(s.GetGroupDomainNodes().front(), 9u);
        }
    }

    Y_UNIT_TEST(LayoutOverridePreservesTargetEligibilityChecks) {
        TSetup s = MakeBlock42Setup();
        s.SetNodeRack(9, "rack-2");
        s.SetPDiskDriveStatus(9, NKikimrBlobStorage::BROKEN);
        TReallocateOptions options{
            .ErasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block,
            .NumFailDomains = 8,
            .TargetPDiskId = NBsController::TPDiskId(9, 1),
            .IgnoreGroupLayoutChecks = true,
        };
        UNIT_ASSERT_EXCEPTION(Reallocate(s, NodeIds({}), false, options),
                              NStorage::TDistributedConfigKeeper::TExConfigError);
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupGeneration(), 1u);
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

    Y_UNIT_TEST(PDiskLocationFilterRestrictsStaticPlacement) {
        TSetup s = MakeSetup();
        s.SetNodeRack(3, "selected-rack");
        auto* settings = s.Config.MutableSelfManagementConfig();
        settings->MutableGeometry()->CopyFrom(Geometry(1));
        auto* filter = settings->AddPDiskFilter();
        filter->AddProperty()->SetType(NKikimrBlobStorage::ROT);
        filter->AddProperty()->MutableLocation()->SetRack("selected-rack");
        NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        keeper.AllocateStaticGroup({
            .Config = &s.Config,
            .GroupId = TGroupId::FromValue(0),
            .GroupGeneration = 2,
            .GroupType = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
            .ReplacedDisks = {{TVDiskIdShort(0, 0, 0), NBsController::TPDiskId()}},
            .BaseConfig = &s.BaseConfig,
            .IgnoreVSlotQuotaCheck = true,
            .AllowUnusableDisks = true,
        });
        UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk().NodeId, 3u);
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

    // static VDisk placed on the PDisk of BSC takes slot id below the range BSC allocates from, as BSC may allocate the
    // next one of its range there (VSlots being deleted are not even listed in base config); only when static ids have
    // reached that range, the slot ids listed are taken into account
    Y_UNIT_TEST(StaticSlotIdsStayBelowBscRange) {
        for (const ui32 staticSlotId : {0u, 1000u}) {
            TSetup s = MakeSetup();
            auto *vslot = s.BaseConfig.AddVSlot(); // VSlot of BSC on the target PDisk
            vslot->MutableVSlotId()->SetNodeId(3);
            vslot->MutableVSlotId()->SetPDiskId(1);
            vslot->MutableVSlotId()->SetVSlotId(1005);
            vslot->SetGroupId(0x80000001);
            vslot->SetGroupGeneration(1);
            vslot->SetStatus("READY");
            auto *group = s.BaseConfig.AddGroup();
            group->SetGroupId(0x80000001);
            group->SetGroupGeneration(1);
            group->SetGroupSizeInUnits(1);
            if (staticSlotId) { // static VDisk placed by older version in the range of BSC
                auto *destroyed = s.Config.MutableBlobStorageConfig()->MutableServiceSet()->AddVDisks();
                VDiskIDFromVDiskID(TVDiskID(TGroupId::FromValue(5), 1, 0, 0, 0), destroyed->MutableVDiskID());
                destroyed->MutableVDiskLocation()->SetNodeID(3);
                destroyed->MutableVDiskLocation()->SetPDiskID(1);
                destroyed->MutableVDiskLocation()->SetVDiskSlotID(staticSlotId);
                destroyed->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
            }
            NKikimr::NStorage::TDistributedConfigKeeper::TStaticGroupReassignments reassignments;
            Reallocate(s, NodeIds({}), false, {
                .TargetPDiskId = NBsController::TPDiskId(3, 1),
                .Reassignments = &reassignments,
            });
            const auto it = reassignments.find(TVDiskIdShort(0, 0, 0));
            UNIT_ASSERT(it != reassignments.end() && it->second.TargetSlotId);
            UNIT_ASSERT_VALUES_EQUAL(it->second.TargetSlotId->GetVSlotId(), staticSlotId ? 1006u : 1u);
        }
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

    Y_UNIT_TEST(UsesReportedSlotCountForStaticGroupReassignment) {
        struct TCase {
            ui32 ConfiguredSlotCount;
            std::optional<ui32> ReportedSlotCount;
            bool CanReassign;
        };
        const TCase cases[] = {
            {1, 2, true},
            {2, 1, false},
            {2, 0, false},
            {0, 2, true},
            {1, std::nullopt, false},
            {2, std::nullopt, true},
        };

        for (const ui64 expectedSlotSize : {0ull, 100ull}) {
            for (const auto& test : cases) {
                TSetup s = MakeSetup();
                for (auto& pdisk : *s.BaseConfig.MutablePDisk()) {
                    if (pdisk.GetNodeId() == 2) {
                        pdisk.SetExpectedSlotCount(test.ConfiguredSlotCount);
                        pdisk.MutablePDiskConfig()->SetExpectedSlotCount(test.ConfiguredSlotCount);
                        pdisk.SetExpectedSlotSize(expectedSlotSize);
                        auto *metrics = pdisk.MutablePDiskMetrics();
                        if (test.ReportedSlotCount) {
                            metrics->SetExpectedSlotCount(*test.ReportedSlotCount);
                        }
                        metrics->SetSlotSizeInUnits(2);
                    }
                }

                // A two-unit group occupies one inferred slot on the target.
                auto *group = s.BaseConfig.AddGroup();
                group->SetGroupId(0x80000001);
                group->SetGroupGeneration(1);
                group->SetGroupSizeInUnits(2);
                auto *vslot = s.BaseConfig.AddVSlot();
                vslot->MutableVSlotId()->SetNodeId(2);
                vslot->MutableVSlotId()->SetPDiskId(1);
                vslot->MutableVSlotId()->SetVSlotId(7);
                vslot->SetGroupId(group->GetGroupId());
                vslot->SetGroupGeneration(1);
                vslot->SetStatus("READY");
                group->AddVSlotId()->CopyFrom(vslot->GetVSlotId());

                const auto reassign = [&] {
                    Reallocate(s, NodeIds({2}), true);
                };
                if (test.CanReassign) {
                    UNIT_ASSERT_NO_EXCEPTION(reassign());
                    UNIT_ASSERT_VALUES_EQUAL(s.GetGroupVDiskPDisk().NodeId, 2u);
                } else {
                    UNIT_ASSERT_EXCEPTION(reassign(), NStorage::TDistributedConfigKeeper::TExConfigError);
                    UNIT_ASSERT_VALUES_EQUAL(s.GetGroupGeneration(), 1u);
                }
            }
        }
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
